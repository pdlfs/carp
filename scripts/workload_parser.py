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
        return sum(p.stat().st_size for p in pathlib.Path(path).rglob('*'))

def get_header(fstream):
    (so_char_bits, so_short_int, so_int, so_float, so_double)  = struct.unpack("ccccc", fstream.read(5))

    cafe_b = fstream.read(2)
    deadbeef_b = fstream.read(4)
    (cafe,) = struct.unpack("h", cafe_b)
    (deadbeef,) = struct.unpack("i", deadbeef_b)

    float_1 = struct.unpack("f", fstream.read(4))
    double_1 = struct.unpack("d", fstream.read(8))

    v_0 = struct.unpack("i", fstream.read(4))
    dump_type = struct.unpack("i", fstream.read(4))

    ( step, ) = struct.unpack("i", fstream.read(4))

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

    ( rank, ) = struct.unpack("i", fstream.read(4))
    ( nproc, ) = struct.unpack("i", fstream.read(4))

    sp_id = struct.unpack("i", fstream.read(4))
    q_m = struct.unpack("f", fstream.read(4))

    return {'rank': rank, 'nproc': nproc, 'step': step}

def get_array_header(fstream):
    (so_p0, ) = struct.unpack("i", fstream.read(4))
    (ndim, ) = struct.unpack("i", fstream.read(4))

    dim_array = []

    for idx in range(ndim):
        (dim_elem, ) = struct.unpack("i", fstream.read(4))
        dim_array.append(dim_elem)

    print(so_p0, ndim, dim_array)
    return {'psize': so_p0, 'nelem': dim_array[0]}

def parse(file_in, file_out):
    print("Parsing %s to %s..." % (file_in, file_out))

    f = open(file_in, "rb")
    g = open(file_out, "wb")

    header = get_header(f)
    print(header)

    array_header = get_array_header(f)
    print(array_header)

    #dxdydz,i,uxuyuz,w,tag,tag
    for i in range(array_header['nelem']):
        particle = struct.unpack("fffiffffqq", f.read(48))
        px, py, pz = particle[4], particle[5], particle[6]
        energy = (1 + px*px + py*py + pz*pz) ** 0.5 - 1
        g.write(struct.pack("f", energy))

    f.close()
    g.close()
    return

def parse_wrap(x):
    parse(x[0], x[1])

def run_job(par_dir, cur_idx, tot_idx):
    all_paths_in = glob.glob(par_dir + "/**/eparticle*")
    all_paths_in = sorted(all_paths_in)
    print(len(all_paths_in))

    if tot_idx != 1:
        print(cur_idx, tot_idx)
        all_paths_in = all_paths_in[cur_idx::tot_idx]

    print(len(all_paths_in))
    all_paths_out = [ s.replace("/particle", "/particle.compressed") for s in all_paths_in]

    for path in all_paths_out:
      pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    #  print("Parsing %s..." % (file_in))
    path_pairs = zip(all_paths_in, all_paths_out)

    with Pool(64) as p:
        p.map(parse_wrap, path_pairs)

def run_realtime(par_dir):
    print('Polling', par_dir, '...')
    ts_seen = {}
    ts_size = {}
    while True:
        time.sleep(60)

        # get all timesteps ready for pocessing
        try:
            contents = os.listdir(par_dir)
        except Exception as e:
            contents = []

        timesteps = sorted([i for i in contents if i.startswith('T.')])
        timesteps = [i for i in timesteps if i not in ts_seen]

        if len(timesteps) == 0: continue

        # pick one
        ts_cur = timesteps[0]
        tspath_cur = par_dir + '/' + ts_cur

        # track it to make sure it has stopped growing
        size_cur = get_size(tspath_cur)
        size_prev = 0
        if tspath_cur in ts_size:
            size_prev = ts_size[tspath_cur]

        ts_size[tspath_cur] = size_cur

        print('Tracking {0}, sz_cur: {1}, sz_prev: {2}'
              .format(ts_cur, size_cur, size_prev))

        if size_cur == size_prev:
            # it has stopped growing, process it
            print('Compacting ', tspath_cur)
            # compact
            run_job(tspath_cur)
            # delete orig
            rmtree(tspath_cur)
            ts_seen[ts_cur] = True


def get_exp_stats():
    result = subprocess.run(['/share/testbed/bin/emulab-listall'], stdout=subprocess.PIPE)
    hoststr = str(result.stdout.decode('ascii'))
    hoststr = hoststr.strip().split(',')
    num_hosts = len(hoststr)
    our_id = open('/var/emulab/boot/nickname', 'r').read().split('.')[0][1:]
    our_id = int(our_id)

    print(our_id, num_hosts)

    return (our_id, num_hosts)

if __name__ == "__main__":
    par_dir = sys.argv[1]
    cur_idx, tot_idx = get_exp_stats()
    run_job(par_dir, cur_idx, tot_idx)
    #run_realtime(par_dir)
