# Coursework submission checklist

**Coursework:** University of Edinburgh Distributed Systems — Building a Parameter Server  
**Deadline (specification):** **4 December 2026**  
**Deliverables:** code repository or ZIP/TAR, informal report **≤8 pages**, recorded demonstration **≤10 minutes**.

## 1. Repository and code ZIP

The implementation and final audit fixes are kept in draft PR [#1](https://github.com/DwayneNixon/ds-coursework/pull/1) until reviewed. **Do not zip old `main` while the draft PR remains unmerged.** The ZIP you submit must correspond to the exact revision that passed the final workflows.

For a clean local checkout of the reviewed ref:

```bash
git clone https://github.com/DwayneNixon/ds-coursework.git
cd ds-coursework
git switch audit/fix-runner-paths
python -m pip install -r requirements.txt
python -m scripts.check_repository
python -m scripts.run_all_regressions
```

After checking every requirement and the PR, merge it if approved and build/download a ZIP of the final commit. Use `git archive`, GitHub's *Code → Download ZIP* or the university's instructed format. **Include source, generated Protobuf bindings, requirements, reproducible configs, tests, harness, docs and experiment evidence.** Exclude `__pycache__`, virtualenvs, generated local secrets and transient crash logs.

## 2. Informal report ≤8 pages

Run after generating canonical X1/X2/X4 figures:

```bash
python -m pip install -r requirements-report.txt
python -m scripts.build_report_pdf
```

Output: `submission/parameter_server_report.pdf`. The builder checks the resulting PDF is ≤8 A4 pages. Inspect **all** pages and plots before submission; verify final regression revision, trial counts and caveats, and add any author details your course requires.

Source: `docs/REPORT_DRAFT.md`. Architecture/evidence notes: `docs/ARCHITECTURE.md`, `docs/EXPERIMENTS.md` and `docs/REQUIREMENTS_AUDIT.md`.

## 3. Ten-minute demonstration

Record the actual required **server add and server kill under load**, not only static screenshots. Use `docs/DEMO.md` for a timed 10-minute walkthrough. Show the manager, worker activity, X5 2→4 migration, X6 confirmed primary crash, continued Pushes and independent offline oracle evidence. Ensure the recorded video is your own and is within the time limit. No recording is generated automatically by these scripts.

## 4. Before uploading to Learn

- [ ] Final audit branch code reviewed and final commit chosen
- [ ] Full A–F CI and integrated X1–X7 smoke checks PASS at that code revision
- [ ] Reproducible X1, X2, X3, X4, X5, X6 and X7 outputs/plots accessible
- [ ] Byte scope, fault scope and intentional X7 fail-closed row explicitly disclosed
- [ ] Report PDF opened, all figures legible, page count ≤8
- [ ] Recorded demo opened, audio/video audible and runtime ≤10 minutes
- [ ] ZIP/TAR excludes local secrets and bytecode and builds from clean checkout
- [ ] Upload code, report and recording in the formats actually permitted by Learn
