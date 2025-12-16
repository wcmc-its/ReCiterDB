#!/bin/bash
# ============================================================================
# ReCiter Nightly Indexing Job - EKS Pod Orchestration Script
# ============================================================================
#
# This script orchestrates the nightly indexing job for ReCiter.
# It should be triggered from an EKS pod after upstream jobs complete.
#
# Prerequisites:
#   - MySQL/MariaDB client installed
#   - Environment variables set: DB_HOST_DEV, DB_USERNAME_DEV, DB_PASSWORD_DEV, DB_NAME_DEV
#
# Usage:
#   ./run_nightly_indexing.sh [--wait-for-upstream] [--dry-run] [--restore]
#
# ============================================================================

set -o pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/logs}"
LOG_FILE="${LOG_DIR}/nightly_indexing_$(date +%Y%m%d_%H%M%S).log"
MAX_RETRIES=3
RETRY_DELAY=60
UPSTREAM_CHECK_INTERVAL=300  # 5 minutes
UPSTREAM_TIMEOUT=7200        # 2 hours

# Database connection using environment variables
DB_HOST="${URL}"
DB_USER="${DB_USERNAME}"
DB_PASS="${DB_PASSWORD}"
DB_NAME="${DB_NAME}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# Functions
# ============================================================================

log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${timestamp} [${level}] ${message}" | tee -a "${LOG_FILE}"
}

log_info() { log "INFO" "$*"; }
log_warn() { log "${YELLOW}WARN${NC}" "$*"; }
log_error() { log "${RED}ERROR${NC}" "$*"; }
log_success() { log "${GREEN}SUCCESS${NC}" "$*"; }

# Check required environment variables
check_env() {
    local missing=0
    for var in DB_HOST DB_USER DB_PASS DB_NAME; do
        if [ -z "${!var}" ]; then
            log_error "Missing required environment variable: $var"
            missing=1
        fi
    done
    return $missing
}

# Test database connectivity
test_db_connection() {
    log_info "Testing database connection..."
    #if mysql -h "${DB_HOST}" -u "${DB_USERNAME}" -p"${DB_PASSWORD}" -e "SELECT 1" "${DB_NAME}" > /dev/null 2>&1; then
    if mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "SELECT 1"; then
        log_success "Database connection successful"
        return 0
    else
        log_error "Database connection failed"
        return 1
    fi
}

# Run a MySQL command and capture output
run_mysql() {
    mysql -h "${DB_HOST}" -u "${DB_USER}" -p"${DB_PASS}" "${DB_NAME}" -N -e "$1" 2>&1
}

# Check if upstream jobs have completed
# Looks for recent updates in person_article and analysis_nih tables
check_upstream_complete() {
    log_info "Checking if upstream jobs have completed..."

    # Check person_article has recent data
    local person_article_count=$(run_mysql "SELECT COUNT(*) FROM person_article WHERE datePublicationAddedToEntrez > DATE_SUB(NOW(), INTERVAL 7 DAY)")
    log_info "Recent person_article records: ${person_article_count}"

    # Check analysis_nih has data
    local nih_count=$(run_mysql "SELECT COUNT(*) FROM analysis_nih")
    log_info "analysis_nih records: ${nih_count}"

    if [ "${person_article_count:-0}" -gt 0 ] && [ "${nih_count:-0}" -gt 0 ]; then
        log_success "Upstream jobs appear complete"
        return 0
    else
        log_warn "Upstream jobs may not be complete"
        return 1
    fi
}

# Wait for upstream jobs to complete
wait_for_upstream() {
    log_info "Waiting for upstream jobs to complete (timeout: ${UPSTREAM_TIMEOUT}s)..."

    local elapsed=0
    while [ $elapsed -lt $UPSTREAM_TIMEOUT ]; do
        if check_upstream_complete; then
            return 0
        fi
        log_info "Waiting ${UPSTREAM_CHECK_INTERVAL}s before next check..."
        sleep $UPSTREAM_CHECK_INTERVAL
        elapsed=$((elapsed + UPSTREAM_CHECK_INTERVAL))
    done

    log_error "Timed out waiting for upstream jobs"
    return 1
}

# Get current status of analysis_summary tables
get_current_status() {
    log_info "Current analysis_summary table status:"
    run_mysql "CALL check_analysis_summary_status()" | while read line; do
        log_info "  $line"
    done
}

# Poll progress from the database
poll_progress() {
    local last_id=0
    local current_id
    local row

    while true; do
        # Get new log entries since last check
        local results=$(run_mysql "SELECT id, step, substep, status, IFNULL(rows_affected,''), IFNULL(message,''), created_at FROM analysis_job_log WHERE id > ${last_id} ORDER BY id")

        if [ -n "$results" ]; then
            echo "$results" | while IFS=$'\t' read -r id step substep status rows msg ts; do
                if [ -n "$id" ]; then
                    if [ -n "$rows" ] && [ "$rows" != "NULL" ]; then
                        log_info "  [${status}] ${step} > ${substep} (rows: ${rows}) ${msg}"
                    else
                        log_info "  [${status}] ${step} > ${substep} ${msg}"
                    fi
                    last_id=$id
                fi
            done
            # Update last_id from the last row
            current_id=$(echo "$results" | tail -1 | cut -f1)
            if [ -n "$current_id" ]; then
                last_id=$current_id
            fi
        fi

        # Check if job is still running (look for SUCCESS or ERROR in recent entries)
        local final_status=$(run_mysql "SELECT status FROM analysis_job_log WHERE id = (SELECT MAX(id) FROM analysis_job_log)")
        if [ "$final_status" = "SUCCESS" ] || [ "$final_status" = "ERROR" ]; then
            break
        fi

        sleep 3
    done
}

# Run the main indexing procedure
run_indexing() {
    local attempt=1

    while [ $attempt -le $MAX_RETRIES ]; do
        log_info "Running populateAnalysisSummaryTables_v2 (attempt ${attempt}/${MAX_RETRIES})..."
        log_info "Polling progress every 3 seconds..."

        local start_time=$(date +%s)

        # Clear old job logs for this run
        run_mysql "DELETE FROM analysis_job_log WHERE created_at < DATE_SUB(NOW(), INTERVAL 1 DAY)" > /dev/null 2>&1

        # Get the current max ID before starting
        local start_log_id=$(run_mysql "SELECT IFNULL(MAX(id),0) FROM analysis_job_log")

        # Run the stored procedure in background
        mysql -h "${DB_HOST}" -u "${DB_USER}" -p"${DB_PASS}" "${DB_NAME}" -e "CALL populateAnalysisSummaryTables_v2()" > /dev/null 2>&1 &
        local mysql_pid=$!

        # Poll for progress while the procedure runs
        local last_id=${start_log_id:-0}
        while kill -0 $mysql_pid 2>/dev/null; do
            # Get new log entries
            local results=$(run_mysql "SELECT id, step, substep, status, IFNULL(rows_affected,''), IFNULL(message,'') FROM analysis_job_log WHERE id > ${last_id} ORDER BY id")

            if [ -n "$results" ]; then
                echo "$results" | while IFS=$'\t' read -r id step substep status rows msg; do
                    if [ -n "$id" ] && [ "$id" != "id" ]; then
                        if [ -n "$rows" ] && [ "$rows" != "" ]; then
                            log_info "  [${status}] ${step} > ${substep} (rows: ${rows}) ${msg}"
                        else
                            log_info "  [${status}] ${step} > ${substep} ${msg}"
                        fi
                    fi
                done
                # Update last_id
                current_id=$(echo "$results" | tail -1 | cut -f1)
                if [ -n "$current_id" ] && [ "$current_id" != "id" ]; then
                    last_id=$current_id
                fi
            fi
            sleep 3
        done

        # Wait for mysql process to finish and get exit code
        wait $mysql_pid
        local exit_code=$?

        # Get any final log entries
        local results=$(run_mysql "SELECT id, step, substep, status, IFNULL(rows_affected,''), IFNULL(message,'') FROM analysis_job_log WHERE id > ${last_id} ORDER BY id")
        if [ -n "$results" ]; then
            echo "$results" | while IFS=$'\t' read -r id step substep status rows msg; do
                if [ -n "$id" ] && [ "$id" != "id" ]; then
                    if [ -n "$rows" ] && [ "$rows" != "" ]; then
                        log_info "  [${status}] ${step} > ${substep} (rows: ${rows}) ${msg}"
                    else
                        log_info "  [${status}] ${step} > ${substep} ${msg}"
                    fi
                fi
            done
        fi

        local end_time=$(date +%s)
        local duration=$((end_time - start_time))

        # Check final status from log table
        local final_status=$(run_mysql "SELECT status FROM analysis_job_log WHERE job_id = (SELECT MAX(job_id) FROM analysis_job_log) ORDER BY id DESC LIMIT 1")

        if [ "$final_status" = "SUCCESS" ]; then
            log_success "Indexing completed successfully in ${duration} seconds"
            return 0
        fi

        log_error "Indexing attempt ${attempt} failed (status: ${final_status}, exit code: ${exit_code})"

        if [ $attempt -lt $MAX_RETRIES ]; then
            log_info "Waiting ${RETRY_DELAY}s before retry..."
            sleep $RETRY_DELAY
        fi

        attempt=$((attempt + 1))
    done

    log_error "All ${MAX_RETRIES} indexing attempts failed"
    return 1
}

# Restore from backup
restore_from_backup() {
    log_warn "Restoring analysis_summary tables from backup..."

    local result=$(run_mysql "CALL restore_from_backup_v2()")
    local exit_code=$?

    echo "$result" | while read line; do
        log_info "  $line"
    done

    if [ $exit_code -eq 0 ] && echo "$result" | grep -q "SUCCESS"; then
        log_success "Restore completed successfully"
        return 0
    else
        log_error "Restore failed"
        return 1
    fi
}

# Validate the results after indexing
validate_results() {
    log_info "Validating indexing results..."

    # Check row counts
    local author_count=$(run_mysql "SELECT COUNT(*) FROM analysis_summary_author")
    local article_count=$(run_mysql "SELECT COUNT(*) FROM analysis_summary_article")
    local person_count=$(run_mysql "SELECT COUNT(*) FROM analysis_summary_person")

    log_info "Row counts - author: ${author_count}, article: ${article_count}, person: ${person_count}"

    # Validation: should have reasonable row counts
    if [ "${author_count:-0}" -lt 100 ] || [ "${article_count:-0}" -lt 100 ] || [ "${person_count:-0}" -lt 10 ]; then
        log_error "Validation failed: Row counts too low"
        return 1
    fi

    # Check h-index computation
    local hindex_computed=$(run_mysql "SELECT COUNT(*) FROM analysis_summary_person WHERE hindexNIH IS NOT NULL OR hindexScopus IS NOT NULL")
    log_info "Persons with h-index computed: ${hindex_computed}"

    log_success "Validation passed"
    return 0
}

# Send notification (placeholder - implement based on your notification system)
send_notification() {
    local status="$1"
    local message="$2"

    # Implement your notification logic here
    # Examples: Slack webhook, email, PagerDuty, etc.

    log_info "Notification: [${status}] ${message}"

    # Example Slack webhook (uncomment and configure):
    # curl -X POST -H 'Content-type: application/json' \
    #     --data "{\"text\":\"ReCiter Indexing [${status}]: ${message}\"}" \
    #     "${SLACK_WEBHOOK_URL}"
}

# Print usage
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    --wait-for-upstream    Wait for upstream jobs (person_article, analysis_nih) to complete
    --dry-run              Check prerequisites but don't run indexing
    --restore              Restore from backup tables instead of running indexing
    --status               Show current status and exit
    -h, --help             Show this help message

Environment Variables Required:
    DB_HOST_DEV            Database host
    DB_USERNAME_DEV        Database username
    DB_PASSWORD_DEV        Database password
    DB_NAME_DEV            Database name

Optional Environment Variables:
    LOG_DIR                Log directory (default: /var/log/reciter)
    SLACK_WEBHOOK_URL      Slack webhook for notifications

Examples:
    # Run indexing immediately
    ./run_nightly_indexing.sh

    # Wait for upstream jobs then run
    ./run_nightly_indexing.sh --wait-for-upstream

    # Just check status
    ./run_nightly_indexing.sh --status

    # Restore from backup after failed run
    ./run_nightly_indexing.sh --restore
EOF
}

# ============================================================================
# Main
# ============================================================================

main() {
    local wait_upstream=0
    local dry_run=0
    local do_restore=0
    local show_status=0

    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            --wait-for-upstream)
                wait_upstream=1
                ;;
            --dry-run)
                dry_run=1
                ;;
            --restore)
                do_restore=1
                ;;
            --status)
                show_status=1
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
        shift
    done

    # Create log directory
    mkdir -p "${LOG_DIR}" 2>/dev/null || true

    log_info "=============================================="
    log_info "ReCiter Nightly Indexing Job Starting"
    log_info "=============================================="
    log_info "Host: $(hostname)"
    log_info "Date: $(date)"
    log_info "Log: ${LOG_FILE}"

    # Check environment
    if ! check_env; then
        log_error "Environment check failed"
        exit 1
    fi

    # Test database connection
    if ! test_db_connection; then
        log_error "Cannot connect to database"
        send_notification "ERROR" "Database connection failed"
        exit 1
    fi

    # Status only mode
    if [ $show_status -eq 1 ]; then
        get_current_status
        exit 0
    fi

    # Restore mode
    if [ $do_restore -eq 1 ]; then
        if restore_from_backup; then
            send_notification "SUCCESS" "Restored from backup"
            exit 0
        else
            send_notification "ERROR" "Restore failed"
            exit 1
        fi
    fi

    # Wait for upstream if requested
    if [ $wait_upstream -eq 1 ]; then
        if ! wait_for_upstream; then
            log_error "Upstream jobs not ready"
            send_notification "ERROR" "Upstream jobs timeout"
            exit 1
        fi
    fi

    # Dry run mode
    if [ $dry_run -eq 1 ]; then
        log_info "Dry run mode - prerequisites checked, not running indexing"
        get_current_status
        exit 0
    fi

    # Show current status before running
    get_current_status

    # Run the indexing
    if run_indexing; then
        # Validate results
        if validate_results; then
            log_success "Nightly indexing completed successfully"
            get_current_status
            send_notification "SUCCESS" "Nightly indexing completed"
            exit 0
        else
            log_error "Validation failed - attempting restore"
            if restore_from_backup; then
                send_notification "WARN" "Indexing failed validation, restored from backup"
            else
                send_notification "CRITICAL" "Indexing failed and restore failed"
            fi
            exit 1
        fi
    else
        log_error "Indexing failed after all retries"
        log_info "Attempting automatic restore from backup..."
        if restore_from_backup; then
            send_notification "WARN" "Indexing failed, restored from backup"
        else
            send_notification "CRITICAL" "Indexing failed and restore failed"
        fi
        exit 1
    fi
}

main "$@"