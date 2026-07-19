NVCC      := nvcc
CFLAGS    := -O2 -arch=sm_87
LDFLAGS   := -lcublas
TARGET    := matmul
SRC       := matmul.cu

.PHONY: all clean profile profile-kernel

all: $(TARGET)

$(TARGET): $(SRC)
	$(NVCC) $(CFLAGS) -o $@ $< $(LDFLAGS)

# System-wide timeline (kernel launches, memcpys, GPU utilization)
profile: $(TARGET)
	nsys profile --stats=true -o matmul_timeline ./$(TARGET)
	@echo "\n==> Open matmul_timeline.nsys-rep in Nsight Systems GUI for the timeline view"

# Per-kernel deep dive (occupancy, memory throughput, warp stalls)
profile-kernel: $(TARGET)
	ncu --set full --target-processes all -o matmul_kernels ./$(TARGET)
	@echo "\n==> Open matmul_kernels.ncu-rep in Nsight Compute GUI for kernel analysis"

clean:
	rm -f $(TARGET) *.nsys-rep *.sqlite *.ncu-rep
