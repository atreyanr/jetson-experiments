# Roadmap

## Jetson LLM Systems Lab

Train and optimize a small GPT-style model entirely on Jetson Orin Nano 8GB.

### Phase 0 — Environment Validation
- [x] Record JetPack, CUDA, PyTorch, Triton versions
- [x] Smoke test (CUDA, BF16 matmul, Triton kernel)
- [x] CI integration

### Phase 1 — First GPT Run (Days 1–3)
- [ ] Clone and run Karpathy nanoGPT
- [ ] Train on Tiny Shakespeare (10M params, BF16)
- [ ] Save checkpoint, generate text
- [ ] Record benchmarks (loss, tokens/sec, memory)

### Phase 2 — Own Transformer (Week 1–2)
- [ ] Rewrite GPT-2 from scratch
- [ ] Shape tests for every component
- [ ] Validate against nanoGPT baseline

### Phase 3 — Modern Architecture (Week 3–4)
- [ ] Implement RMSNorm, RoPE, SwiGLU
- [ ] GPT-2 vs Llama benchmark comparison

### Phase 4 — Profiling (Month 2)
- [ ] PyTorch profiler, Nsight Systems, Nsight Compute
- [ ] Identify top 3 bottlenecks

### Phase 5 — Triton Kernels (Month 3)
- [ ] RMSNorm, softmax, RoPE, cross-entropy, SwiGLU
- [ ] Benchmark against PyTorch baselines

### Phase 6 — Liger Study (Month 4)
- [ ] Three-way benchmark: PyTorch vs Triton vs Liger

### Phase 7 — Edge Deployment (Month 5)
- [ ] FP16/INT8 quantization, TensorRT, KV cache
- [ ] Interactive chat demo on Jetson

## CUDA Kernel Experiments (Foundational)

- [x] Matrix multiplication (naive, tiled, cuBLAS comparison)
- [ ] FFT experiments (cuFFTDx on-device)
