import pandas as pd

def read_manifest(dpath, epoch, rank):
    fpath = '{0}/vpic-manifest.{1}.{2}'.format(dpath, epoch, rank)
    f = open(fpath, 'r').read().splitlines()
    mf_items = []
    for item in f:
        item = item.split(' ')
        item = [float(item[0]), float(item[1]), int(item[3]), int(item[4])]
        mf_items.append(item)

    return mf_items

def read_full_manifest(dpath, epoch):
    all_items = []
    for rank in range(512):
        items = read_manifest(dpath, epoch, rank)
        all_items += items

    return all_items


def overlap(x1, y1, x2, y2):
    if x1 < x2 and y1 > y2:
        return True

    if x1 >= x2 and x1 <= y2:
        return True

    if y1 >= x2 and y1 <= y2:
        return True

    return False


def analyze_manifest(mf_items, qbeg, qend):
    mass_match = 0
    mass_oob = 0

    for item in mf_items:
        #  print(item[0], item[1])
        if overlap(item[0], item[1], qbeg, qend):
            mass_match += item[2]
            mass_oob += item[3]

    print('Match: {0}, OOB: {1}'.format(mass_match, mass_oob))

def analyze_actual_manifest(dpath, rank, epoch, rbeg, rend):
    fpath = '{0}/vpic-manifest.{1}'.format(dpath, rank)

    cols = ['epoch', 'offset', 'xo', 'yo', 'xe', 'ye', 'cnt', 'cntoob', 'rno']
    df = pd.read_csv(fpath, header=None, names=cols)
    df = df.reset_index()

    mass_match = 0
    mass_oob = 0

    for idx, row in df.iterrows():
        #  print(row)
        xo, yo = row['xo'], row['yo']
        #  print(xo, yo, rbeg, rend)
        if overlap(xo, yo, rbeg, rend):
            mass_match += int(row['cnt'])
            mass_oob += int(row['cntoob'])

    print('Match: {0}, OOB: {1}'.format(mass_match, mass_oob))


def run():
    dpath_base = '/mnt/ltio/jobs-big-run/vpic-carp8m/carp_P3584M_intvl500000/plfs'
    dpath_mockmf = dpath_base + '/manifests'
    dpath_partmf = dpath_base + '/particle'

    epoch = 0
    rank = 252
    qbeg = 1.205
    qend = 1.306

    #  mf_items = read_manifest(dpath_mockmf, epoch, rank)
    mf_items = read_full_manifest(dpath_mockmf, epoch)
    analyze_manifest(mf_items, qbeg, qend)

    analyze_actual_manifest(dpath_partmf, rank, epoch, qbeg, qend)

if __name__ == '__main__':
    run()
