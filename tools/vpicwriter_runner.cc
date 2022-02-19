#include <H5Part.h>
#include <iostream>

#define BEGIN_PRIMITIVE do
#define END_PRIMITIVE while(0)
#define _UTIL_STRINGIFY(s)#s
#define EXPAND_AND_STRINGIFY(s)_UTIL_STRINGIFY(s)
#define _SIM_LOG_PREFIX \
    __FILE__ "(" EXPAND_AND_STRINGIFY(__LINE__) ")[" << rank_ << "]: "
#define sim_log_local(x) std::cerr << _SIM_LOG_PREFIX << x << std::endl
#define sim_log(x) BEGIN_PRIMITIVE {         \
    if( rank_==0 ) {                                  \
          std::cerr << _SIM_LOG_PREFIX << x << std::endl;  \
          std::cerr.flush();                               \
        }                                                  \
} END_PRIMITIVE

class VPICWriter {
 public:
  VPICWriter() {}
  void Hello() {}
  void Run() {
    H5PartFile* h5pf;
    h5part_float32_t* Pf;
    h5part_int32_t* Pi;
    h5part_int64_t ierr;
    h5part_int64_t step;
    h5part_int64_t np_local;
    const char* fname = "abcd";
    h5pf = H5PartOpenFileParallel(fname, H5PART_WRITE, MPI_COMM_WORLD);

    ierr = H5PartSetStep(h5pf, step);
    ierr = H5PartSetNumParticlesStrided(h5pf, np_local, 8);
    if (ierr != H5PART_SUCCESS)
      sim_log(
          "Error occured in setting the # of tracers; Error code: " << ierr);

    ierr = H5PartWriteDataFloat32(h5pf, "dX", Pf);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing dX in tracer; Error code: " << ierr);
    // sim_log("Finished writing dX");

    ierr = H5PartWriteDataFloat32(h5pf, "dY", Pf + 1);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing dY in tracer; Error code: " << ierr);
    // sim_log("Finished writing dY");

    ierr = H5PartWriteDataFloat32(h5pf, "dZ", Pf + 2);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing dZ in tracer; Error code: " << ierr);
    // sim_log("Finished writing dZ");

    ierr = H5PartWriteDataInt32(h5pf, "i", Pi + 3);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing i in tracer; Error code: " << ierr);
    // sim_log("Finished writing i");

    ierr = H5PartWriteDataFloat32(h5pf, "Ux", Pf + 4);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing Ux in tracer; Error code: " << ierr);
    // sim_log("Finished writing Ux");

    ierr = H5PartWriteDataFloat32(h5pf, "Uy", Pf + 5);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing Uy in tracer; Error code: " << ierr);
    // sim_log("Finished writing Uy");

    ierr = H5PartWriteDataFloat32(h5pf, "Uz", Pf + 6);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing Uz in tracer; Error code: " << ierr);
    // sim_log("Finished writing Uz");

    // ierr = H5PartWriteDataFloat32 (h5pf, "q", Pf+7);
    ierr = H5PartWriteDataInt32(h5pf, "q", Pi + 7);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in writing q in tracer; Error code: " << ierr);

    ierr = H5PartCloseFile(h5pf);
    if (ierr != H5PART_SUCCESS)
      sim_log("Error occured in closing " << fname << ". Error code: " << ierr);
  }

 private:
  int rank_;
};

int main() { return 0; }
