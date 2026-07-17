# Jetson Orin Development Learnings

Practical gotchas and solutions discovered while working on NVIDIA Jetson Orin Nano 8GB (JetPack 7.2, CUDA 13.2, Ubuntu 24.04).

---

## 1. PyTorch Installation (JetPack 7.2+)

JetPack 7.2 dropped Jetson-specific PyTorch wheels. The old NVIDIA wheel indexes (`developer.download.nvidia.com/compute/redist/jp/...`, `pypi.jetson-ai-lab.io/jp6/cu126`) only cover up to JetPack 6.x. Searching for "Jetson PyTorch wheel" leads to stale instructions.

**Fix:** Use standard upstream SBSA packages from the official PyTorch index:

```bash
pip3 install torch torchvision --index-url https://download.pytorch.org/whl/cu132
```

**Prerequisites:**
- `libcusparseLt.so.0` must be installed — PyTorch 24.06+ requires cuSPARSELt
- Use a venv with `--system-site-packages` to pick up CUDA libs

**CC 8.7 warning:** PyTorch may warn about "no published builds supporting this GPU." This is cosmetic — the CC 8.0 fallback works. Only rebuild from source with `TORCH_CUDA_ARCH_LIST="8.7"` if you hit an actual kernel failure.

**Measured performance (Orin Nano 8GB, 8 SMs):**

| Dtype | TFLOPS |
|-------|--------|
| BF16  | 8.3    |
| FP16  | 6.0    |
| TF32  | 3.9    |
| FP32  | 0.8    |
| INT8  | 2.3    |

Memory bandwidth: 62 GB/s (shared CPU/GPU unified memory). Compute-bound sweet spot: matrix dim >= 1024. Use BF16 or FP16 for neural workloads.

---

## 2. Python Venv System Package Conflicts

System-installed Python packages at `/usr/lib/python3/dist-packages/` leak into venv environments and conflict with pip-installed versions. Most common with matplotlib — causes `AttributeError` on `RcParams`, `Axes3D`, or `matplotlib_inline`.

**Fix:** Strip system paths before importing. Place at the very top of the notebook/script, before any other imports:

```python
import sys
sys.path = [p for p in sys.path if "/usr/lib/python3" not in p]
```

Then restart the kernel.

---

## 3. CUDA Profiling with Nsight Systems

Wall-clock timing with `cudaEventElapsedTime` is misleading. First-call overhead from `cudaMalloc` and library initialization dominates — cuBLAS can appear 30x slower than its actual kernel time.

**Commands:**

```bash
# System-wide timeline (kernel launches, memcpys, API overhead)
nsys profile --stats=true -o output_name ./your_binary

# Per-kernel deep dive (occupancy, warp stalls, memory throughput)
ncu --set full --target-processes all -o output_name ./your_binary
```

**Key tables in nsys output:**

| Table | What it shows |
|-------|---------------|
| `cuda_gpu_kern_sum` | Actual GPU kernel execution time (the truth) |
| `cuda_api_sum` | CPU-side API call time (reveals malloc/init overhead) |
| `cuda_gpu_mem_time_sum` | Memory transfer costs |
| `cuda_gpu_mem_size_sum` | Bytes moved |

**Real example (1024x1024 matmul on Orin Nano):**
- cuBLAS wall-clock: 86 ms (looks slow)
- cuBLAS actual kernel (`ampere_sgemm_128x64_nn`): 2.65 ms (8x faster than naive)
- `cudaMalloc` alone: 214 ms (73.6% of all CUDA API time)

**Unified memory optimization:** Orin shares physical memory between CPU and GPU. Use `cudaMallocManaged` instead of `cudaMalloc` + `cudaMemcpy` to eliminate transfer overhead entirely.

**Warm-up pattern** for fair comparisons:

```cpp
dummy_kernel<<<1, 1>>>();
cudaDeviceSynchronize();
// now start timing
```

---

## 4. Node.js Installation (No Root)

Jetson doesn't ship with Node.js. If `sudo` requires a password, use nvm:

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
nvm install 22
```

Restart Claude Code after installing so hooks pick up the new `node` binary.
