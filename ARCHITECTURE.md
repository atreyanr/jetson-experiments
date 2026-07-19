# Architecture

## Experiment structure

Each experiment lives in its own directory with a self-contained build system:

```
<experiment>/
├── Makefile          — build, run, profile targets
├── *.cu             — CUDA source files
├── *.h              — headers (if needed)
└── README.md        — experiment description and results (optional)
```

## Profiling workflow

1. `make profile` generates an nsys timeline (`.nsys-rep`)
2. `make profile-kernel` generates ncu kernel analysis (`.ncu-rep`)
3. Reports go in `profiles/` for cross-experiment comparison

## Conventions

- All kernels target SM 8.7 (Jetson Orin Nano)
- Timing uses nsys kernel summaries (not cold-start cudaEvent)
- Unified memory (`cudaMallocManaged`) preferred over explicit transfers
- Benchmarks compare against cuBLAS/cuFFT baselines where applicable
