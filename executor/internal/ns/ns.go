package ns

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"lambda/internal/mount"
	"lambda/internal/params"
)

func Start(p params.Params) *params.Result {
	fdCHW := make([]int32, 2)
	_, _, errno := syscall.Syscall(
		syscall.SYS_PIPE,
		uintptr(unsafe.Pointer(&fdCHW[0])),
		0,
		0,
	)
	if errno != 0 {
		println("Error:", errno)
		return nil
	}

	_, _, errno = syscall.Syscall(
		syscall.SYS_FCNTL,
		uintptr(fdCHW[1]),
		uintptr(syscall.F_SETFD),
		uintptr(syscall.FD_CLOEXEC),
	)

	fdPW := make([]int32, 2)
	_, _, errno = syscall.Syscall(
		syscall.SYS_PIPE,
		uintptr(unsafe.Pointer(&fdPW[0])),
		0,
		0,
	)

	stdout := make([]int32, 2)
	_, _, errno = syscall.Syscall(
		syscall.SYS_PIPE,
		uintptr(unsafe.Pointer(&stdout[0])),
		0,
		0,
	)

	if errno != 0 {
		println("Error:", errno)
		return nil
	}

	r1, _, errno := syscall.Syscall(
		syscall.SYS_CLONE,
		p.Flags,
		0,
		0,
	)
	if errno != 0 {
		println("Error:", errno)
		return nil
	}

	var result *params.Result
	if r1 == 0 {
		result = childEntry(p, stdout, fdCHW, fdPW)
	} else {
		result = parentEntry(p, stdout, fdCHW, fdPW, r1)
	}
	return result

}

func childEntry(p params.Params, stdout, fdCHW, fdPW []int32) *params.Result {
	syscall.Syscall(
		syscall.SYS_CLOSE,
		uintptr(fdCHW[0]),
		0,
		0,
	)

	syscall.Syscall(
		syscall.SYS_CLOSE,
		uintptr(fdPW[1]),
		0,
		0,
	)

	syscall.Syscall(
		syscall.SYS_CLOSE,
		uintptr(stdout[0]),
		0,
		0,
	)

	syscall.Syscall(
		syscall.SYS_DUP2,
		uintptr(stdout[1]),
		uintptr(1),
		0,
	)

	tmp := []byte{0}
	syscall.Syscall(
		syscall.SYS_READ,
		uintptr(fdPW[0]),
		uintptr(unsafe.Pointer(&tmp[0])),
		1,
	)

	mount.PivotRoot(p)

	mount.MountProc()

	mount.Cleanup()

	execInto(p)

	return nil
}

func execInto(p params.Params) {
	selfPathpath, _ := syscall.BytePtrFromString(p.CodeSource)
	argv, _ := syscall.SlicePtrFromStrings([]string{""})

	env := []string{}
	envp, _ := syscall.SlicePtrFromStrings(env)

	_, _, errno := syscall.Syscall(
		syscall.SYS_EXECVE,
		uintptr(unsafe.Pointer(selfPathpath)),
		uintptr(unsafe.Pointer(&argv[0])),
		uintptr(unsafe.Pointer(&envp[0])),
	)
	if errno != 0 {
		fmt.Println("Error executing program:", errno)
	}
}

func writeProcFile(path string, value string) error {
	pathPtr, _ := syscall.BytePtrFromString(path)
	fdMap, _, errno := syscall.Syscall(
		syscall.SYS_OPEN,
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(syscall.O_WRONLY),
		0,
	)
	defer syscall.Syscall(
		syscall.SYS_CLOSE,
		fdMap,
		0,
		0,
	)
	if errno != 0 {
		return errno
	}
	syscall.Syscall(
		syscall.SYS_WRITE,
		uintptr(fdMap),
		uintptr(unsafe.Pointer(&[]byte(value)[0])),
		uintptr(len(value)),
	)
	return nil
}

func parentEntry(p params.Params, stdout, fdCHW, fdPW []int32, chID uintptr) *params.Result {
	syscall.Syscall(
		syscall.SYS_CLOSE,
		uintptr(fdCHW[1]),
		0,
		0,
	)

	syscall.Syscall(
		syscall.SYS_CLOSE,
		uintptr(fdPW[0]),
		0,
		0,
	)

	syscall.Syscall(
		syscall.SYS_CLOSE,
		uintptr(stdout[1]),
		0,
		0,
	)

	pidStr := strconv.Itoa(int(chID))
	uidMapPath := "/proc/" + pidStr + "/uid_map"
	setgroupsPath := "/proc/" + pidStr + "/setgroups"
	gidMapPath := "/proc/" + pidStr + "/gid_map"
	err := writeProcFile(setgroupsPath, "deny")
	if err != nil {
		fmt.Printf("Error writing to setgroups: %v\n", err)
		return nil
	}

	uid, _, _ := syscall.Syscall(syscall.SYS_GETUID, 0, 0, 0)
	err = writeProcFile(uidMapPath, fmt.Sprintf("0 %d 1", uid))
	if err != nil {
		fmt.Printf("Error writing to uid_map: %v\n", err)
		return nil
	}

	gid, _, _ := syscall.Syscall(syscall.SYS_GETGID, 0, 0, 0)
	err = writeProcFile(gidMapPath, fmt.Sprintf("0 %d 1", gid))
	if err != nil {
		fmt.Printf("Error writing to gid_map: %v\n", err)
		return nil
	}

	cgroupPath := "/sys/fs/cgroup/otter-" + pidStr
	err = syscall.Mkdir(cgroupPath, 0755)
	if err != nil {
		fmt.Printf("Error creating cgroup directory: %v\n", err)
		return nil
	}
	if p.MemoryLimit > 0 {
		writeProcFile(cgroupPath+"/memory.max", fmt.Sprintf("%d", p.MemoryLimit))
	}
	if p.CPUQuota > 0 {
		writeProcFile(cgroupPath+"/cpu.max", fmt.Sprintf("%d 100000", p.CPUQuota))
	}
	writeProcFile(cgroupPath+"/cgroup.procs", pidStr)

	syscall.Syscall(
		syscall.SYS_WRITE,
		uintptr(fdPW[1]),
		uintptr(unsafe.Pointer(&[]byte{1}[0])),
		1,
	)

	tmp := []byte{0}
	syscall.Syscall(
		syscall.SYS_READ,
		uintptr(fdCHW[0]),
		uintptr(unsafe.Pointer(&tmp[0])),
		1,
	)

	result := params.Result{
		ExitCode: 0,
		TimedOut: false,
	}

	var stdoutBuffer bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			//f, _ := os.OpenFile("output.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			buf := make([]byte, 1024)
			n, _, _ := syscall.Syscall(
				syscall.SYS_READ,
				uintptr(stdout[0]),
				uintptr(unsafe.Pointer(&buf[0])),
				uintptr(len(buf)),
			)
			if n == 0 {
				break
			}
			//f.Write(buf[:n])
			stdoutBuffer.Write(buf[:n])
		}
	}()

	done := make(chan struct{})

	go func() {
		var status uint32
		syscall.Syscall(syscall.SYS_WAIT4, chID, uintptr(unsafe.Pointer(&status)), 0)
		exitCode := int((status >> 8) & 0xff)
		result.ExitCode = exitCode
		close(done)
	}()

	if p.TimeLimit > 0 {
		select {
		case <-done:
		case <-time.After(p.TimeLimit):
			syscall.Syscall(
				syscall.SYS_KILL,
				chID,
				uintptr(syscall.SIGKILL),
				0,
			)
			<-done
			result.TimedOut = true
		}
	} else {
		<-done
	}

	wg.Wait()
	result.Stdout = stdoutBuffer.Bytes()

	syscall.Syscall6(syscall.SYS_CLOSE, uintptr(fdCHW[0]), 0, 0, 0, 0, 0)
	return &result
}
