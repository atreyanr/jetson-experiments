# jetson-experiments

Lab notebook for GPU/CUDA experiments on an NVIDIA Jetson Orin Nano 8GB.

## Hardware

- NVIDIA Jetson Orin Nano 8GB (8 SMs, CC 8.7)
- JetPack R39.2 / CUDA 13.2 / Ubuntu 24.04
- Unified CPU/GPU memory (62 GB/s shared bandwidth)

## Repository layout

```
experiments/   — self-contained experiment directories (CUDA C++, Python)
benchmarks/    — benchmark results and comparisons
profiles/      — nsys/ncu profiling reports and analysis
notes/         — research notes, papers, ideas
docs/          — project documentation and learnings
```

## Quick start

### Python notebooks

```bash
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook
```

### CUDA C++ (per-experiment directory)

```bash
cd cuda-matmul
make
./matmul
```

### Profiling

```bash
make profile          # nsys timeline
make profile-kernel   # ncu per-kernel analysis
```

## Git workflow

- Default branch: `dev`
- Feature branches: `exp/<topic>` or `feat/<topic>`
- Conventional commits: `feat:`, `fix:`, `chore:`, `docs:`
