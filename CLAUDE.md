# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Lab notebook for GPU/CUDA experiments on an NVIDIA Jetson Orin Nano 8GB. Two kinds of artifacts live here:

1. **Jupyter notebooks** (Python + PyTorch) — compute benchmarks, algorithm prototyping, visualization
2. **CUDA C++ programs** — hand-written kernels benchmarked against cuBLAS baselines

Each experiment is self-contained in its own directory or notebook.

## Hardware target

Jetson Orin Nano 8GB — 8 SMs, CC 8.7, unified CPU/GPU memory (62 GB/s shared bandwidth). JetPack R39.2 / CUDA 13.2 / Ubuntu 24.04. All code in this repo targets this device.

## Build & run commands

### Python notebooks

```bash
source .venv/bin/activate
jupyter notebook                      # launches browser UI
```

Python deps: `pip install -r requirements.txt`. PyTorch is **not** in requirements.txt — install separately with the SBSA wheel:

```bash
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu132
```

### CUDA C++ (per-experiment directory)

```bash
cd experiments/cuda-matmul
make              # builds with nvcc -O2 -arch=sm_87, links -lcublas
./matmul          # default 1024x1024; pass N as arg: ./matmul 2048
make clean        # removes binary + profiler artifacts
```

### Profiling

```bash
make profile          # nsys system-wide timeline → matmul_timeline.nsys-rep
make profile-kernel   # ncu per-kernel analysis → matmul_kernels.ncu-rep
```

## Jetson-specific gotchas

See `docs/jetson-learnings.md` for the full list. The critical ones:

- **sys.path stripping**: system `dist-packages` leak into venvs. Every script/notebook must start with `sys.path = [p for p in sys.path if "dist-packages" not in p or ".venv" in p]` before other imports. Do NOT filter `/usr/lib/python3` broadly — that removes stdlib modules like `ctypes`.
- **cuBLAS first-call overhead**: `cudaMalloc` and library init dominate wall-clock timing. Use nsys `cuda_gpu_kern_sum` for real kernel times, not `cudaEventElapsedTime` on cold runs.
- **Unified memory**: prefer `cudaMallocManaged` over `cudaMalloc` + `cudaMemcpy` on this device — same physical memory, no transfer.

## Git workflow

- **Default branch**: `dev` (all PRs target `dev`, not `main`)
- **Never commit directly to `dev`** — always use feature branches
- **Conventional commits**: `feat:`, `fix:`, `chore:`, `docs:`, etc.
- **Branch naming**: `exp/<topic>` for experiments, `feat/<topic>` for features

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues on `atreyanr/jetson-experiments`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-label vocabulary. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout — one `CONTEXT.md` + `docs/adr/` at repo root. See `docs/agents/domain.md`.

### Spec-driven development

All non-trivial work starts from a spec. See `docs/agents/spec-workflow.md`.
