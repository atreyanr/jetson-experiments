#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <cstdio>
#include <cstdlib>
#include <cmath>

#define TILE_SIZE 16

#define CHECK_CUDA(call)                                                       \
    do {                                                                       \
        cudaError_t err = call;                                                \
        if (err != cudaSuccess) {                                              \
            fprintf(stderr, "CUDA error at %s:%d: %s\n", __FILE__, __LINE__,  \
                    cudaGetErrorString(err));                                   \
            exit(1);                                                           \
        }                                                                      \
    } while (0)

// Kernel 1: Naive — one thread computes one element of C
__global__ void matmul_naive(const float *A, const float *B, float *C,
                             int N) {
    int row = blockIdx.y * blockDim.y + threadIdx.y;
    int col = blockIdx.x * blockDim.x + threadIdx.x;
    if (row < N && col < N) {
        float sum = 0.0f;
        for (int k = 0; k < N; ++k)
            sum += A[row * N + k] * B[k * N + col];
        C[row * N + col] = sum;
    }
}

// Kernel 2: Shared-memory tiled — tiles loaded cooperatively, reused per block
__global__ void matmul_tiled(const float *A, const float *B, float *C,
                             int N) {
    __shared__ float sA[TILE_SIZE][TILE_SIZE];
    __shared__ float sB[TILE_SIZE][TILE_SIZE];

    int row = blockIdx.y * TILE_SIZE + threadIdx.y;
    int col = blockIdx.x * TILE_SIZE + threadIdx.x;
    float sum = 0.0f;

    for (int t = 0; t < (N + TILE_SIZE - 1) / TILE_SIZE; ++t) {
        int a_col = t * TILE_SIZE + threadIdx.x;
        int b_row = t * TILE_SIZE + threadIdx.y;

        sA[threadIdx.y][threadIdx.x] =
            (row < N && a_col < N) ? A[row * N + a_col] : 0.0f;
        sB[threadIdx.y][threadIdx.x] =
            (b_row < N && col < N) ? B[b_row * N + col] : 0.0f;

        __syncthreads();

        for (int k = 0; k < TILE_SIZE; ++k)
            sum += sA[threadIdx.y][k] * sB[k][threadIdx.x];

        __syncthreads();
    }

    if (row < N && col < N)
        C[row * N + col] = sum;
}

void fill_random(float *data, int n) {
    for (int i = 0; i < n; ++i)
        data[i] = (float)(rand() % 100) / 100.0f;
}

float max_error(const float *a, const float *b, int n) {
    float mx = 0.0f;
    for (int i = 0; i < n; ++i)
        mx = fmaxf(mx, fabsf(a[i] - b[i]));
    return mx;
}

int main(int argc, char **argv) {
    int N = 1024;
    if (argc > 1) N = atoi(argv[1]);
    printf("Matrix size: %d x %d\n", N, N);

    size_t bytes = N * N * sizeof(float);

    float *h_A = (float *)malloc(bytes);
    float *h_B = (float *)malloc(bytes);
    float *h_C_naive = (float *)malloc(bytes);
    float *h_C_tiled = (float *)malloc(bytes);
    float *h_C_cublas = (float *)malloc(bytes);

    srand(42);
    fill_random(h_A, N * N);
    fill_random(h_B, N * N);

    float *d_A, *d_B, *d_C;
    CHECK_CUDA(cudaMalloc(&d_A, bytes));
    CHECK_CUDA(cudaMalloc(&d_B, bytes));
    CHECK_CUDA(cudaMalloc(&d_C, bytes));
    CHECK_CUDA(cudaMemcpy(d_A, h_A, bytes, cudaMemcpyHostToDevice));
    CHECK_CUDA(cudaMemcpy(d_B, h_B, bytes, cudaMemcpyHostToDevice));

    dim3 threads(TILE_SIZE, TILE_SIZE);
    dim3 blocks((N + TILE_SIZE - 1) / TILE_SIZE,
                (N + TILE_SIZE - 1) / TILE_SIZE);

    cudaEvent_t start, stop;
    CHECK_CUDA(cudaEventCreate(&start));
    CHECK_CUDA(cudaEventCreate(&stop));
    float ms;

    // --- Naive kernel ---
    CHECK_CUDA(cudaEventRecord(start));
    matmul_naive<<<blocks, threads>>>(d_A, d_B, d_C, N);
    CHECK_CUDA(cudaEventRecord(stop));
    CHECK_CUDA(cudaEventSynchronize(stop));
    CHECK_CUDA(cudaEventElapsedTime(&ms, start, stop));
    CHECK_CUDA(cudaMemcpy(h_C_naive, d_C, bytes, cudaMemcpyDeviceToHost));
    float gflops_naive = (2.0f * N * N * N) / (ms * 1e6f);
    printf("\nNaive    : %8.2f ms  |  %6.2f GFLOPS\n", ms, gflops_naive);

    // --- Tiled kernel ---
    CHECK_CUDA(cudaMemset(d_C, 0, bytes));
    CHECK_CUDA(cudaEventRecord(start));
    matmul_tiled<<<blocks, threads>>>(d_A, d_B, d_C, N);
    CHECK_CUDA(cudaEventRecord(stop));
    CHECK_CUDA(cudaEventSynchronize(stop));
    CHECK_CUDA(cudaEventElapsedTime(&ms, start, stop));
    CHECK_CUDA(cudaMemcpy(h_C_tiled, d_C, bytes, cudaMemcpyDeviceToHost));
    float gflops_tiled = (2.0f * N * N * N) / (ms * 1e6f);
    printf("Tiled    : %8.2f ms  |  %6.2f GFLOPS\n", ms, gflops_tiled);

    // --- cuBLAS ---
    CHECK_CUDA(cudaMemset(d_C, 0, bytes));
    cublasHandle_t handle;
    cublasCreate(&handle);
    float alpha = 1.0f, beta = 0.0f;
    CHECK_CUDA(cudaEventRecord(start));
    cublasSgemm(handle, CUBLAS_OP_N, CUBLAS_OP_N,
                N, N, N, &alpha, d_B, N, d_A, N, &beta, d_C, N);
    CHECK_CUDA(cudaEventRecord(stop));
    CHECK_CUDA(cudaEventSynchronize(stop));
    CHECK_CUDA(cudaEventElapsedTime(&ms, start, stop));
    CHECK_CUDA(cudaMemcpy(h_C_cublas, d_C, bytes, cudaMemcpyDeviceToHost));
    float gflops_cublas = (2.0f * N * N * N) / (ms * 1e6f);
    printf("cuBLAS   : %8.2f ms  |  %6.2f GFLOPS\n", ms, gflops_cublas);
    cublasDestroy(handle);

    printf("\nMax error (naive  vs cuBLAS): %.6f\n",
           max_error(h_C_naive, h_C_cublas, N * N));
    printf("Max error (tiled  vs cuBLAS): %.6f\n",
           max_error(h_C_tiled, h_C_cublas, N * N));

    cudaFree(d_A);
    cudaFree(d_B);
    cudaFree(d_C);
    free(h_A);
    free(h_B);
    free(h_C_naive);
    free(h_C_tiled);
    free(h_C_cublas);
    cudaEventDestroy(start);
    cudaEventDestroy(stop);

    return 0;
}
