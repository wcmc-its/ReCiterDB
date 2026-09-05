#!/usr/bin/env bash
# Checks the deploy gate in k8-buildspec.yml: pushing an image and repointing the live
# cronjob must happen ONLY for a CodePipeline-initiated build on master/dev with a
# cronjob name set.
#
# Why this exists: BRANCH and CRON_JOB_NAME come from the pipeline's Build action, not
# from the CodeBuild project. The project default is BRANCH=dev with no CRON_JOB_NAME,
# so before this gate a manual `aws codebuild start-build --source-version <anything>`
# satisfied the old ".*dev" test and pushed that branch's image into the PROD ECR repo.
#
# Run: bash tests/test_buildspec_deploy_gate.sh
set -uo pipefail
cd "$(dirname "$0")/.."

# The gate, lifted verbatim from k8-buildspec.yml's pre_build phase. Kept in sync by the
# drift check at the end, which fails if the buildspec's copy stops matching this one.
gate() {
  CODEBUILD_INITIATOR="$1" BRANCH="$2" CRON_JOB_NAME="$3" bash -s <<'SH'
case "$CODEBUILD_INITIATOR" in
  codepipeline/*) DEPLOY=yes ;;
  *)              DEPLOY=no ;;
esac
if [ "$DEPLOY" = yes ]; then
  expr "${BRANCH}" : ".*master" >/dev/null || expr "${BRANCH}" : ".*dev" >/dev/null || DEPLOY=no
  [ -n "${CRON_JOB_NAME}" ] || { echo "CRON_JOB_NAME is empty -- refusing to deploy" >&2; DEPLOY=no; }
fi
echo "$DEPLOY"
SH
}

fails=0
check() {  # check <expected> <initiator> <branch> <cronjob> <description>
  local want="$1" got
  got=$(gate "$2" "$3" "$4" 2>/dev/null)
  if [ "$got" = "$want" ]; then
    echo "[OK]   deploy=$got  $5"
  else
    echo "[FAIL] deploy=$got want=$want  $5"
    fails=$((fails + 1))
  fi
}

# The one path that must still deploy: the real pipeline.
check yes "codepipeline/ReCiterDB" "master" "reciterdb" "pipeline on master with a cronjob name (the live prod path)"
check yes "codepipeline/ReCiterDB-dev" "dev" "reciterdb-dev" "pipeline on dev"

# The foot-gun this gate closes. 'reciter' is the literal initiator seen on the manual
# build that prompted it, which inherited the project default BRANCH=dev.
check no "reciter" "dev" "" "manual start-build inheriting BRANCH=dev and no cronjob name"
check no "reciter" "dev" "reciterdb" "manual start-build even if a cronjob name is somehow present"
check no "reciter" "master" "reciterdb" "manual start-build claiming master"
check no "GitHub-Hookshot/abc123" "master" "reciterdb" "webhook-triggered build (validates only, never deploys)"
check no "codebuild/ReCiterDB" "master" "reciterdb" "another CodeBuild project chaining in"

# Pipeline builds that must still be refused.
check no "codepipeline/ReCiterDB" "feature/whatever" "reciterdb" "pipeline pointed at a feature branch"
check no "codepipeline/ReCiterDB" "master" "" "pipeline with CRON_JOB_NAME unset (a malformed kubectl target)"

# Guard against the test drifting from the buildspec it claims to cover.
for needle in \
  'codepipeline/\*) DEPLOY=yes' \
  'expr "\${BRANCH}" : ".\*master"' \
  '\[ -n "\${CRON_JOB_NAME}" \]' \
  'if \[ "\$DEPLOY" = yes \]; then'
do
  if grep -qE "$needle" k8-buildspec.yml; then
    echo "[OK]   buildspec still contains: $needle"
  else
    echo "[FAIL] buildspec no longer contains: $needle -- this test is stale"
    fails=$((fails + 1))
  fi
done

# The old unguarded conditions must be gone from the build/deploy steps.
if grep -qE '^\s+if expr "\$\{BRANCH\}"' k8-buildspec.yml; then
  echo "[FAIL] a bare BRANCH-only deploy condition is still present"
  fails=$((fails + 1))
else
  echo "[OK]   no bare BRANCH-only deploy condition remains"
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "SELFTEST PASS"
else
  echo "SELFTEST FAIL ($fails)"
  exit 1
fi
