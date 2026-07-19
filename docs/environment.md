# Environment

Validated 2026-07-19.

## Hardware

| Component | Value |
|-----------|-------|
| Device | NVIDIA Jetson Orin Nano 8GB |
| GPU | Orin (8 SMs, CC 8.7) |
| GPU Memory | 7,485 MB (unified with CPU) |
| Memory Bandwidth | 62 GB/s (shared) |
| Peak Compute (BF16) | 8.3 TFLOPS |
| Peak Compute (FP32) | 0.8 TFLOPS |
| Swap | 16 GB configured |

## Software

| Component | Version |
|-----------|---------|
| OS | Ubuntu 24.04.4 LTS (Noble Numbat) |
| JetPack | 7.2 (R39.2) |
| CUDA | 13.2 (V13.2.78) |
| Python | 3.12.3 |
| PyTorch | 2.13.0+cu132 (SBSA wheel) |
| Triton | 3.7.1 (ARM64 native, ships with PyTorch) |
| TensorRT | 10.16.2.10 |

## Known issues

### PyTorch CC 8.7 warning

PyTorch 2.13 emits a warning that CC 8.7 is excluded from its support matrix:

```
Found GPU0 Orin which is of compute capability (CC) 8.7.
No published PyTorch CUDA builds for release 2.13.0+cu132 support this GPU.
```

This is cosmetic — PyTorch falls back to CC 8.0 PTX and everything works correctly.
Suppress with `warnings.filterwarnings('ignore')` or `PYTHONWARNINGS=ignore`.

### sys.path contamination

System `dist-packages` leak into venvs on Jetson. Filter them out while
keeping stdlib and venv packages:

```python
import sys
sys.path = [p for p in sys.path if "dist-packages" not in p or ".venv" in p]
```

### Unified memory

CPU and GPU share the same physical memory. `cudaMallocManaged` is preferred over
`cudaMalloc` + `cudaMemcpy` — there is no transfer, just page migration.
The 7,485 MB GPU memory budget is shared with the OS and CPU allocations.

## Monitoring

```bash
tegrastats                              # comprehensive Jetson stats
cat /sys/devices/gpu.0/load             # GPU utilization (0-1000)
watch -n 1 free -m                      # memory pressure
python -c "import torch; print(torch.cuda.max_memory_allocated())"  # peak PyTorch alloc
```
