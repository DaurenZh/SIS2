package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise the actual main/exit status in an isolated subprocess, without
// making the test runner itself exit or reusing its global flag definitions.
func TestServerProcess(t *testing.T) {
	if os.Getenv("SIS2_STARTUP_TEST") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			break
		}
	}
	flag.CommandLine = flag.NewFlagSet("sis2-server", flag.ExitOnError)
	main()
	os.Exit(0)
}
func serverProcess(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	root := t.TempDir()
	defaults := []string{"-test.run=^TestServerProcess$", "--", "-db", filepath.Join(root, "metadata.db"), "-objects", filepath.Join(root, "objects"), "-grpc-addr", "127.0.0.1:0", "-rest-addr", "127.0.0.1:0"}
	cmd := exec.Command(os.Args[0], append(defaults, args...)...)
	cmd.Env = append(os.Environ(), "SIS2_STARTUP_TEST=1", "MINIO_ENDPOINT=")
	return cmd
}
func TestBusyPortFailsStartupWithoutReadiness(t *testing.T) {
	for _, portFlag := range []string{"-grpc-addr", "-rest-addr", "-metrics-addr"} {
		t.Run(portFlag, func(t *testing.T) {
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			ready := filepath.Join(t.TempDir(), "ready.pid")
			cmd := serverProcess(t, portFlag, occupied.Addr().String(), "-ready-file", ready)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("busy port exited successfully: %s", output)
			}
			if !bytes.Contains(output, []byte("address already in use")) {
				t.Fatalf("unexpected startup failure: %s", output)
			}
			if _, err = os.Stat(ready); !os.IsNotExist(err) {
				t.Fatalf("failed server announced readiness: %v", err)
			}
		})
	}
}
func TestReadinessIdentifiesProcessAndIsRemovedOnShutdown(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready.pid")
	cmd := serverProcess(t, "-metrics-addr", "127.0.0.1:0", "-ready-file", ready)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(ready)
		if err == nil && strings.TrimSpace(string(data)) == fmt.Sprint(cmd.Process.Pid) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never announced its PID")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("graceful shutdown: %v; %s", err, output.String())
	}
	stopped = true
	if _, err := os.Stat(ready); !os.IsNotExist(err) {
		t.Fatalf("stale readiness file after shutdown: %v", err)
	}
}
