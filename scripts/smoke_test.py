#!/usr/bin/env python3
"""Smoke test: verify CUDA, PyTorch, and Triton work on this Jetson."""

import sys
sys.path = [p for p in sys.path if "dist-packages" not in p or ".venv" in p]

import warnings
warnings.filterwarnings("ignore")


def check_cuda():
    import torch

    assert torch.cuda.is_available(), "CUDA not available"
    name = torch.cuda.get_device_name(0)
    assert "Orin" in name, f"Expected Orin GPU, got: {name}"

    props = torch.cuda.get_device_properties(0)
    mem_mb = props.total_memory / 1024**2
    assert mem_mb > 4000, f"Expected >4 GB GPU memory, got {mem_mb:.0f} MB"

    print(f"CUDA .... OK  ({name}, {mem_mb:.0f} MB, CC {props.major}.{props.minor})")
    return True


def check_bf16_matmul():
    import torch

    a = torch.randn(256, 256, device="cuda", dtype=torch.bfloat16)
    b = torch.randn(256, 256, device="cuda", dtype=torch.bfloat16)
    c = a @ b

    a_f32 = a.float()
    b_f32 = b.float()
    c_ref = a_f32 @ b_f32

    max_err = (c.float() - c_ref).abs().max().item()
    assert max_err < 1.0, f"BF16 matmul error too high: {max_err}"
    print(f"BF16 ... OK  (256x256 matmul, max error={max_err:.4f})")
    return True


def check_triton():
    import torch
    import triton
    import triton.language as tl

    @triton.jit
    def add_kernel(x_ptr, y_ptr, out_ptr, n, BLOCK: tl.constexpr):
        idx = tl.program_id(0) * BLOCK + tl.arange(0, BLOCK)
        mask = idx < n
        x = tl.load(x_ptr + idx, mask=mask)
        y = tl.load(y_ptr + idx, mask=mask)
        tl.store(out_ptr + idx, x + y, mask=mask)

    n = 1024
    x = torch.randn(n, device="cuda")
    y = torch.randn(n, device="cuda")
    out = torch.empty(n, device="cuda")

    grid = lambda meta: (triton.cdiv(n, meta["BLOCK"]),)
    add_kernel[grid](x, y, out, n, BLOCK=256)

    assert torch.allclose(out, x + y, atol=1e-5), "Triton kernel produced wrong results"
    print(f"Triton . OK  (v{triton.__version__}, vector add n={n})")
    return True


def print_versions():
    import torch

    print()
    print("Versions:")
    print(f"  Python   {sys.version.split()[0]}")
    print(f"  PyTorch  {torch.__version__}")
    try:
        import triton
        print(f"  Triton   {triton.__version__}")
    except ImportError:
        print("  Triton   not installed")
    try:
        import tensorrt
        print(f"  TensorRT {tensorrt.__version__}")
    except ImportError:
        print("  TensorRT not installed")


def main():
    passed = 0
    failed = 0

    for name, check in [("CUDA", check_cuda), ("BF16", check_bf16_matmul), ("Triton", check_triton)]:
        try:
            check()
            passed += 1
        except Exception as e:
            print(f"{name} ... FAIL ({e})")
            failed += 1

    print_versions()
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
