# legacy

The harnesses that produced `results/` (egbench, regbench, trainbench,
`bandwidth_benchmark.py`) and the runbooks and plans that drove them. Frozen: kept so
every number in `results/*.md` can still be regenerated from the committed campaign
scripts in `_egbench_ws/ceiling/` and `_bench_ws/job/`, which import these by name.

Run them with `PYTHONPATH=legacy`. `regbench.server` and `regbench.synth` moved to
`tbench/`; the two files here re-export them.

New measurement goes through `tbench/` and a round directory. Do not extend these.
