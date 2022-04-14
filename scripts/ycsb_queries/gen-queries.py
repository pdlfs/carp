import glob
import pandas as pd
import sys

def get_distrib():
    distrib = [int(i) for i in open('zipf.18266').read().split('\n') if len(i)]
    return distrib


def read_manifests(mf_path: str):
    all_mfs = glob.glob(mf_path + '/vpic-manifest.*')
    all_mfs = sorted(all_mfs, key = lambda x: int(x.split('.')[-1]))
    #  all_mfs = all_mfs[:3]
    print('{0} manifest files found'.format(len(all_mfs)))
    all_dfs = []
    col_names = ['epoch', 'offset', 'obsbeg', 'obsend', 'expbeg', 'expend', 'masstot', 'massoob', 'renegrnd'] 
    for idx, mf_path in enumerate(all_mfs):
        df = pd.read_csv(mf_path, header=None, names=col_names)
        df['rank'] = idx
        all_dfs.append(df)

    df = pd.concat(all_dfs, ignore_index=True)
    return df


def gen_queries(mf, distrib, epdel, qwidth, qfname):
    mfsize = 18266
    with open(qfname, 'a+') as f:
        for sidx in distrib:
            eidx = min(sidx + qwidth, mfsize) - 1
            epoch = mf.iloc[sidx][0] - epdel
            qbegin = mf.iloc[sidx][2]
            qend = mf.iloc[eidx][3]
            f.write('{0},{1},{2}\n'.format(epoch, qbegin, qend))


def run():
    mf_path = '/mnt/lt20/jobs-qlat/vpic-carp8m/carp_P3584M_intvl250000/plfs/particle.merged'
    mf = read_manifests(mf_path)
    all_epochs = mf['epoch'].unique()

    distrib = get_distrib()

    params = [
        (0, 'merged'),
        (2, 'orig'),
    ]

    #  widths = [ 20, 50, 100, 500]
    widths = [5, 20, 50, 100]

    for epoch in all_epochs:
        epoch_mf = mf[mf['epoch'] == epoch]
        for epoff, label in params:
            print('Epoch Offset: ', epoff, 'File Label: ', label)
            for width in widths:
                print('Width: ', width)
                qfpath = 'eval.uniform/queries.{0}.{2}.csv'.format(label, epoch, width)
                gen_queries(epoch_mf, distrib, epoff, width, qfpath)


if __name__ == '__main__':
    run()
