NVCC      := nvcc
MATHDX    := $(HOME)/nvidia-mathdx-26.06.0-cuda13/nvidia/mathdx/26.06
CFLAGS    := -O2 -arch=sm_87 -std=c++17
INCLUDES  := -I$(MATHDX)/include -I$(MATHDX)/external/cutlass/include
LDFLAGS   := -lcufft
TARGET    := fft_bench
SRC       := fft_bench.cu

.PHONY: all clean profile

all: $(TARGET)

$(TARGET): $(SRC)
	$(NVCC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LDFLAGS)

profile: $(TARGET)
	nsys profile --stats=true -o fft_timeline ./$(TARGET)
	@echo "\n==> Open fft_timeline.nsys-rep in Nsight Systems GUI"

clean:
	rm -f $(TARGET) *.nsys-rep *.sqlite *.ncu-rep *.qdstrm
