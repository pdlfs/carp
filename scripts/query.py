import glob
import struct, sys
import multiprocessing

def query_worker(fpath: str, qbeg: float, qend: float) -> int:
    hitcnt = 0
    ldata = 0
    maxval = 0

    with open(fpath, 'rb') as f:
        data = f.read()
        ldata = int(len(data) / 4)
        x = struct.unpack('{0}f'.format(ldata), data)
        hitcnt += len([i for i in x if i >= qbeg and i <= qend])
        maxval = max(x)

    rank = int(fpath.split('.')[-1])

    return rank, hitcnt, ldata, maxval


def qworker_wrapper(args):
    return query_worker(args[0], args[1], args[2])


def query(dpath: str, qbeg: float, qend: float) -> int:
    files = glob.glob(dpath + '/*')
    print('File len: ', len(files))
    qbegl = [qbeg] * len(files)
    qendl = [qend] * len(files)
    matchobj = []
    with multiprocessing.Pool(32) as p:
       matchobj = p.map(qworker_wrapper, zip(files, qbegl, qendl))

    ls_match = list(zip(*matchobj))

    ranks = ls_match[0]
    hits = ls_match[1]
    total = ls_match[2]
    max_all = ls_match[3]

    #  for pair in zip(ranks, hits):
        #  print('{0},{1}\n'.format(pair[1], pair[0]))

    tot_hits = sum(hits)
    tot_len = sum(total)
    max_val = max(max_all)

    qsel = tot_hits * 100.0 / tot_len

    print(sorted(hits, reverse=True)[:10])
    print('{} hits, {:2f}% selectivity'.format(tot_hits, qsel))
    print('Max Val: {:2f}'.format(max_val))

    return hits 


def resolve_epoch(dpath: str, epoch: int) -> str:
    all_ts = glob.glob(dpath + '/T.*')
    all_ts = [int(x.split('.')[-1]) for x in all_ts]
    all_ts = sorted(all_ts)
    selected_t = all_ts[epoch]
    t_path = '{0}/T.{1}'.format(dpath, selected_t)
    return t_path


def handle_query(dpath: str, qstr: str) -> None:
    epoch, qbeg, qend = qstr.split(',')
    epoch = int(epoch)
    qbeg = float(qbeg)
    qend = float(qend)

    t_path = resolve_epoch(dpath, epoch)
    print(t_path)
    query(t_path, qbeg, qend)


if __name__ == '__main__':
    dpath = sys.argv[1]
    qstr = sys.argv[2]
    handle_query(dpath, qstr)
