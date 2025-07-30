#!/bin/bash

BASELINE_PROCS="/tmp/baseline_processes.txt"
CLEANUP_LOG="/tmp/cleanup_$(date +%Y%m%d).log"
SCRIPT_PID=$$

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$CLEANUP_LOG"
}

record_baseline() {
    log_message "Recording baseline processes..."
    ps -eo pid,ppid,cmd --no-headers > "$BASELINE_PROCS"
    log_message "Script PID: $SCRIPT_PID"
    log_message "Baseline recorded with $(wc -l < "$BASELINE_PROCS") processes"
}

cleanup_processes() {
    log_message "Starting process cleanup..."
    
    if [[ ! -f "$BASELINE_PROCS" ]]; then
        log_message "ERROR: Baseline file not found!"
        return 1
    fi
    
    current_procs=$(mktemp)
    ps -eo pid,ppid,cmd --no-headers > "$current_procs"
    
    while read -r pid ppid cmd; do
        if [[ "$pid" == "$SCRIPT_PID" ]]; then
            continue
        fi
        
        if ! grep -q "^$pid " "$BASELINE_PROCS"; then
            if [[ -d "/proc/$pid" ]]; then
                log_message "Killing non-baseline process: PID=$pid CMD=$cmd"
                kill -TERM "$pid" 2>/dev/null || true
                sleep 1
                if [[ -d "/proc/$pid" ]]; then
                    kill -KILL "$pid" 2>/dev/null || true
                fi
            fi
        fi
    done < "$current_procs"
    
    rm -f "$current_procs"
    
    pkill -f "python.*app.py" 2>/dev/null || true
    pkill -f "flask" 2>/dev/null || true
    
    fuser -k 5231/tcp 2>/dev/null || true
    
    log_message "Process cleanup completed"
}

main_loop() {
    while true; do
        log_message "Starting master script..."
        
        python master.py
        exit_code=$?
        
        log_message "Master script exited with code: $exit_code"
        
        cleanup_processes
        
        log_message "Restarting in 5 seconds..."
        sleep 5
    done
}

trap 'log_message "Received termination signal, cleaning up..."; cleanup_processes; exit 0' TERM INT

log_message "System Manager Starting..."
record_baseline
main_loop