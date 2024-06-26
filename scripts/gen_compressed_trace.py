import os
import sys
import time
import pathlib
import io
import struct
import glob
import subprocess

from multiprocessing import Pool
from shutil import rmtree


def get_size(path: str) -> int:
    return sum(p.stat().st_size for p in pathlib.Path(path).rglob("*"))


"""
get_header: parses a vpic eparticle file header
and returns relevant info
"""


def get_header(fstream):
    (so_char_bits, so_short_int, so_int, so_float, so_double) = struct.unpack(
        "ccccc", fstream.read(5)
    )

    cafe_b = fstream.read(2)
    deadbeef_b = fstream.read(4)
    (cafe,) = struct.unpack("h", cafe_b)
    (deadbeef,) = struct.unpack("i", deadbeef_b)

    float_1 = struct.unpack("f", fstream.read(4))
    double_1 = struct.unpack("d", fstream.read(8))

    v_0 = struct.unpack("i", fstream.read(4))
    dump_type = struct.unpack("i", fstream.read(4))

    (step,) = struct.unpack("i", fstream.read(4))

    imx = struct.unpack("i", fstream.read(4))
    jmx = struct.unpack("i", fstream.read(4))
    jmx = struct.unpack("i", fstream.read(4))

    grid_dt = struct.unpack("f", fstream.read(4))
    dx = struct.unpack("f", fstream.read(4))
    dy = struct.unpack("f", fstream.read(4))
    dz = struct.unpack("f", fstream.read(4))

    x0 = struct.unpack("f", fstream.read(4))
    y0 = struct.unpack("f", fstream.read(4))
    z0 = struct.unpack("f", fstream.read(4))

    cvac = struct.unpack("f", fstream.read(4))
    eps0 = struct.unpack("f", fstream.read(4))

    zero = struct.unpack("f", fstream.read(4))

    (rank,) = struct.unpack("i", fstream.read(4))
    (nproc,) = struct.unpack("i", fstream.read(4))

    sp_id = struct.unpack("i", fstream.read(4))
    q_m = struct.unpack("f", fstream.read(4))

    return {"rank": rank, "nproc": nproc, "step": step}


"""
get_array_header: parses the array header that
precedes the particle data in a vpic eparticle file
"""


def get_array_header(fstream):
    (so_p0,) = struct.unpack("i", fstream.read(4))
    (ndim,) = struct.unpack("i", fstream.read(4))

    dim_array = []

    for idx in range(ndim):
        (dim_elem,) = struct.unpack("i", fstream.read(4))
        dim_array.append(dim_elem)

    print(so_p0, ndim, dim_array)
    return {"psize": so_p0, "nelem": dim_array[0]}


"""
parse: takes a single vpic eparticle file as input,
and emits a file with ux/uy/uz aggregated into a
single energy value
"""


def parse(file_in, file_out):
    print("Parsing %s to %s..." % (file_in, file_out))

    f = open(file_in, "rb")
    g = open(file_out, "wb")

    header = get_header(f)
    print(header)

    array_header = get_array_header(f)
    print(array_header)

    # dxdydz,i,uxuyuz,w,tag,tag
    for i in range(array_header["nelem"]):
        particle = struct.unpack("fffiffffqq", f.read(48))
        px, py, pz = particle[4], particle[5], particle[6]
        energy = (1 + px * px + py * py + pz * pz) ** 0.5 - 1
        g.write(struct.pack("f", energy))

    f.close()
    g.close()
    return


"""
run_job: takes as input a vpic particle/ directory
and parses and emits a vpic particle.compressed/ directory
using multiprocessing
"""


def run_job(vpic_particle_dir: str, parallelism: int = 32):
    all_paths_in = glob.glob(vpic_particle_dir + "/**/eparticle*")
    all_paths_in = sorted(all_paths_in)

    print("f{len(all_paths_in)} files found")

    all_paths_out = [
        s.replace("/particle", "/particle.compressed") for s in all_paths_in
    ]

    for path in all_paths_out:
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)

    path_pairs = zip(all_paths_in, all_paths_out)

    with Pool(parallelism) as p:
        p.starmap(parse, path_pairs)


if __name__ == "__main__":
    # Run as:
    # python gen_compressed_trace.py /path/to/vpic/particle
    vpic_particle_dir = sys.argv[1]
    run_job(vpic_particle_dir)
