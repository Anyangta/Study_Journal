#!/bin/bash
# refresh.sh -- everything that has to happen after a sweep finishes, in order.
#
# The order matters: analyze.py rebuilds summary.json from the raw run
# directories, model.py fits from that, plot.py draws from the fits, and
# verify.py re-derives the paper's numbers from all three.  Running them out of
# order silently reports the previous sweep's answers.
#
#   ./refresh.sh            full pass, quiet
#   ./refresh.sh -v         full pass, showing each tool's own output
#   ./refresh.sh --quick    skip analyze.py (use when summary.json is current)
cd /home/wontak/icngc2_stream || exit 1

VERBOSE=0
QUICK=0
for a in "$@"; do
    [ "$a" = "-v" ] && VERBOSE=1
    [ "$a" = "--quick" ] && QUICK=1
done
run() {
    echo
    echo "=============================================================="
    echo "== $1"
    echo "=============================================================="
    shift
    if [ "$VERBOSE" = 1 ]; then "$@"; else "$@" 2>&1 | tail -40; fi
}

echo "refresh.sh  $(date '+%F %T')"
echo "runs on disk: $(ls -d results/*/ 2>/dev/null | wc -l)"
for t in m2 m3 m4 m5 m6 m8; do
    d="logs/$t.log.done"
    [ -f "$d" ] && echo "  $t: $(wc -l < "$d") done"
done

[ "$QUICK" = 0 ] && run "analyze.py  (raw runs -> summary.json/csv)" python3 analyze.py

run "drift.py controls  (did the box hold still during the sweep?)" \
    python3 drift.py controls

run "drift.py fit  (steady term, no-failure runs at the reference point)" \
    python3 drift.py fit k200k _r50_ _m0_

run "model.py  (fits + optimum tables)" python3 model.py

run "verify.py  (paper's numbers, re-derived)" python3 verify.py

run "plot.py  (figures)" python3 plot.py

echo
echo "=============================================================="
echo "== next: verify.py --update prints a QUOTED block matching the"
echo "==       current data.  Paste it into verify.py, then update the"
echo "==       paper text to match -- not the other way round."
echo "=============================================================="
