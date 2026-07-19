#include <cuda_runtime.h>
#include <cufft.h>
#include <cufftdx.hpp>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <vector>

#define CHECK_CUDA(call)                                                       \
    do {                                                                       \
        cudaError_t err = call;                                                \
        if (err != cudaSuccess) {                                              \
            fprintf(stderr, "CUDA error at %s:%d: %s\n", __FILE__, __LINE__,  \
                    cudaGetErrorString(err));                                   \
            exit(1);                                                           \
        }                                                                      \
    } while (0)

#define CHECK_CUFFT(call)                                                      \
    do {                                                                       \
        cufftResult err = call;                                                \
        if (err != CUFFT_SUCCESS) {                                            \
            fprintf(stderr, "cuFFT error at %s:%d: %d\n", __FILE__, __LINE__, \
                    err);                                                      \
            exit(1);                                                           \
        }                                                                      \
    } while (0)

// ---------------------------------------------------------------------------
// cuFFTDx block-level FFT kernel
// ---------------------------------------------------------------------------
template<class FFT>
__launch_bounds__(FFT::max_threads_per_block)
__global__ void cufftdx_kernel(typename FFT::value_type* data,
                               typename FFT::workspace_type workspace) {
    using complex_type = typename FFT::value_type;

    complex_type thread_data[FFT::storage_size];

    const unsigned int local_fft_id  = threadIdx.y;
    const unsigned int global_fft_id = blockIdx.x * FFT::ffts_per_block + local_fft_id;
    const unsigned int offset        = cufftdx::size_of<FFT>::value * global_fft_id;
    constexpr unsigned int stride    = FFT::stride;

    unsigned int index = offset + threadIdx.x;
    for (unsigned int i = 0; i < FFT::elements_per_thread; i++) {
        if ((i * stride + threadIdx.x) < cufftdx::size_of<FFT>::value) {
            thread_data[i] = data[index];
            index += stride;
        }
    }

    extern __shared__ __align__(alignof(float4)) complex_type shared_mem[];
    FFT().execute(thread_data, shared_mem, workspace);

    index = offset + threadIdx.x;
    for (unsigned int i = 0; i < FFT::elements_per_thread; i++) {
        if ((i * stride + threadIdx.x) < cufftdx::size_of<FFT>::value) {
            data[index] = thread_data[i];
            index += stride;
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmark cuFFT (library) for a batch of 1D C2C transforms
// ---------------------------------------------------------------------------
float bench_cufft(int fft_size, int batch_count, int warmup, int iters) {
    size_t total_elems = (size_t)fft_size * batch_count;
    size_t bytes = total_elems * sizeof(cufftComplex);

    cufftComplex* d_data;
    CHECK_CUDA(cudaMalloc(&d_data, bytes));

    std::vector<cufftComplex> h_data(total_elems);
    for (size_t i = 0; i < total_elems; i++) {
        h_data[i] = {float(i % fft_size) / fft_size, 0.0f};
    }
    CHECK_CUDA(cudaMemcpy(d_data, h_data.data(), bytes, cudaMemcpyHostToDevice));

    cufftHandle plan;
    CHECK_CUFFT(cufftPlan1d(&plan, fft_size, CUFFT_C2C, batch_count));

    for (int i = 0; i < warmup; i++)
        CHECK_CUFFT(cufftExecC2C(plan, d_data, d_data, CUFFT_FORWARD));
    CHECK_CUDA(cudaDeviceSynchronize());

    cudaEvent_t start, stop;
    CHECK_CUDA(cudaEventCreate(&start));
    CHECK_CUDA(cudaEventCreate(&stop));

    CHECK_CUDA(cudaEventRecord(start));
    for (int i = 0; i < iters; i++)
        CHECK_CUFFT(cufftExecC2C(plan, d_data, d_data, CUFFT_FORWARD));
    CHECK_CUDA(cudaEventRecord(stop));
    CHECK_CUDA(cudaEventSynchronize(stop));

    float ms;
    CHECK_CUDA(cudaEventElapsedTime(&ms, start, stop));

    cufftDestroy(plan);
    cudaFree(d_data);
    cudaEventDestroy(start);
    cudaEventDestroy(stop);

    return ms / iters;
}

// ---------------------------------------------------------------------------
// Benchmark cuFFTDx (device-side) for a batch of 1D C2C transforms
// ---------------------------------------------------------------------------
template<unsigned int FFTSize, unsigned int EPT, unsigned int FPB>
float bench_cufftdx(int batch_count, int warmup, int iters) {
    using namespace cufftdx;

    using FFT = decltype(Size<FFTSize>() + Precision<float>() + Type<fft_type::c2c>()
                         + Direction<fft_direction::forward>()
                         + ElementsPerThread<EPT>() + FFTsPerBlock<FPB>()
                         + SM<870>() + Block());

    using complex_type = typename FFT::value_type;

    const unsigned int fft_size_val  = cufftdx::size_of<FFT>::value;
    const size_t total_elems         = (size_t)fft_size_val * batch_count;
    const size_t bytes               = total_elems * sizeof(complex_type);
    const unsigned int blocks_needed = (batch_count + FFT::ffts_per_block - 1) / FFT::ffts_per_block;

    complex_type* d_data;
    CHECK_CUDA(cudaMalloc(&d_data, bytes));

    std::vector<complex_type> h_data(total_elems);
    for (size_t i = 0; i < total_elems; i++) {
        h_data[i] = complex_type{float(i % fft_size_val) / fft_size_val, 0.0f};
    }
    CHECK_CUDA(cudaMemcpy(d_data, h_data.data(), bytes, cudaMemcpyHostToDevice));

    cudaStream_t stream;
    CHECK_CUDA(cudaStreamCreate(&stream));

    cudaError_t ws_err = cudaSuccess;
    auto workspace = cufftdx::make_workspace<FFT>(ws_err, stream);
    CHECK_CUDA(ws_err);

    CHECK_CUDA(cudaFuncSetAttribute(
        cufftdx_kernel<FFT>,
        cudaFuncAttributeMaxDynamicSharedMemorySize,
        FFT::shared_memory_size));

    for (int i = 0; i < warmup; i++)
        cufftdx_kernel<FFT><<<blocks_needed, FFT::block_dim, FFT::shared_memory_size, stream>>>(d_data, workspace);
    CHECK_CUDA(cudaStreamSynchronize(stream));

    cudaEvent_t start, stop;
    CHECK_CUDA(cudaEventCreate(&start));
    CHECK_CUDA(cudaEventCreate(&stop));

    CHECK_CUDA(cudaEventRecord(start, stream));
    for (int i = 0; i < iters; i++)
        cufftdx_kernel<FFT><<<blocks_needed, FFT::block_dim, FFT::shared_memory_size, stream>>>(d_data, workspace);
    CHECK_CUDA(cudaEventRecord(stop, stream));
    CHECK_CUDA(cudaEventSynchronize(stop));

    float ms;
    CHECK_CUDA(cudaEventElapsedTime(&ms, start, stop));

    cudaFree(d_data);
    cudaStreamDestroy(stream);
    cudaEventDestroy(start);
    cudaEventDestroy(stop);

    return ms / iters;
}

// ---------------------------------------------------------------------------
// 5 * N * log2(N) flops per complex C2C FFT (standard estimate)
// ---------------------------------------------------------------------------
double fft_gflops(int fft_size, int batch_count, float ms) {
    double flops = 5.0 * fft_size * log2((double)fft_size) * batch_count;
    return flops / (ms * 1e6);
}

int main(int argc, char** argv) {
    int batch_count = 4096;
    if (argc > 1) batch_count = atoi(argv[1]);

    const int warmup = 10;
    const int iters  = 100;

    printf("Jetson Orin Nano — cuFFT vs cuFFTDx benchmark\n");
    printf("Batch count: %d\n", batch_count);
    printf("Warmup: %d  |  Iterations: %d\n\n", warmup, iters);

    printf("%-10s  %-10s  %10s  %10s  %10s\n",
           "FFT Size", "Method", "Avg (ms)", "GFLOPS", "Speedup");
    printf("--------------------------------------------------------------\n");

    // --- FFT size 128 ---
    {
        float ms_cufft  = bench_cufft(128, batch_count, warmup, iters);
        float ms_cufftdx = bench_cufftdx<128, 8, 4>(batch_count, warmup, iters);
        printf("%-10d  %-10s  %10.3f  %10.2f\n",
               128, "cuFFT", ms_cufft, fft_gflops(128, batch_count, ms_cufft));
        printf("%-10d  %-10s  %10.3f  %10.2f  %9.2fx\n",
               128, "cuFFTDx", ms_cufftdx, fft_gflops(128, batch_count, ms_cufftdx),
               ms_cufft / ms_cufftdx);
        printf("\n");
    }

    // --- FFT size 256 ---
    {
        float ms_cufft  = bench_cufft(256, batch_count, warmup, iters);
        float ms_cufftdx = bench_cufftdx<256, 8, 2>(batch_count, warmup, iters);
        printf("%-10d  %-10s  %10.3f  %10.2f\n",
               256, "cuFFT", ms_cufft, fft_gflops(256, batch_count, ms_cufft));
        printf("%-10d  %-10s  %10.3f  %10.2f  %9.2fx\n",
               256, "cuFFTDx", ms_cufftdx, fft_gflops(256, batch_count, ms_cufftdx),
               ms_cufft / ms_cufftdx);
        printf("\n");
    }

    // --- FFT size 512 ---
    {
        float ms_cufft  = bench_cufft(512, batch_count, warmup, iters);
        float ms_cufftdx = bench_cufftdx<512, 8, 1>(batch_count, warmup, iters);
        printf("%-10d  %-10s  %10.3f  %10.2f\n",
               512, "cuFFT", ms_cufft, fft_gflops(512, batch_count, ms_cufft));
        printf("%-10d  %-10s  %10.3f  %10.2f  %9.2fx\n",
               512, "cuFFTDx", ms_cufftdx, fft_gflops(512, batch_count, ms_cufftdx),
               ms_cufft / ms_cufftdx);
        printf("\n");
    }

    // --- FFT size 1024 ---
    {
        float ms_cufft  = bench_cufft(1024, batch_count, warmup, iters);
        float ms_cufftdx = bench_cufftdx<1024, 8, 1>(batch_count, warmup, iters);
        printf("%-10d  %-10s  %10.3f  %10.2f\n",
               1024, "cuFFT", ms_cufft, fft_gflops(1024, batch_count, ms_cufft));
        printf("%-10d  %-10s  %10.3f  %10.2f  %9.2fx\n",
               1024, "cuFFTDx", ms_cufftdx, fft_gflops(1024, batch_count, ms_cufftdx),
               ms_cufft / ms_cufftdx);
    }

    return 0;
}
