import wget
import os
import zipfile
import pandas as pd


def download_sf(state, year, span, path):
    """Download summary file. Save it in ../data/ folder."""

    dfile = f"{state}_Tracts_Block_Groups_Only.zip"
    if not os.path.exists("../data/" + dfile):
        url = f"https://www2.census.gov/programs-surveys/acs/summary_file/{year}/data" \
            f"/{span}_year_by_state/" + dfile
        wget.download(url=url, out=path)

    hfile = f"ACS_{span}yr_Seq_Table_Number_Lookup.txt"
    if not os.path.exists("../data/" + hfile):
        url = f"https://www2.census.gov/programs-surveys/acs/summary_file/{year}/documentation/user_tools/" + hfile
        wget.download(url=url, out=path)
    return dfile, hfile


def parse_variable_names(path):
    """Given the path to the lookup txt file, generate lists of variable codes and names."""

    result = list()
    result_index = list()

    with open(path) as f1:
        f1.readline()
        line = (f1.readline()).split(',')

        while line:
            if len(line) < 2:
                break
            if line[5] != '':
                var_def = list([line[7]])
                var_def.append(line[8].strip())
                line = (f1.readline()).split(',')
                var_def.append(line[7].replace("Universe: ", "").strip())
                var_name = list()
                last_level = False

            if line[3] != '':
                if ":" not in line[7] and "--" not in line[7] and len(var_name) == 0:
                    result.append("&&".join([line[7]] + var_def))
                elif ":" not in line[7] and "--" not in line[7] and len(var_name) > 0:
                    if '.' not in line[3]:
                        result.append("&&".join([line[7]] + var_name + var_def))
                    last_level = True
                else:
                    if last_level:
                        var_name.pop(0)
                    var_name.insert(0, line[7])
                    last_level = False
                    if '.' not in line[3]:
                        result.append("&&".join(var_name + var_def))

                if '.' not in line[3]:
                    result_index.append(line[1] + line[2] + line[3].zfill(3))

            line = (f1.readline()).split(',')

    return result_index, result


def denormalize_tocsv(geo_dataframe, zipped_datafile, header, save_path, file_name):
    """Take geo reference dataframe, zipped data file and merge them together iteratively. Apply header and save it in
    provided location."""

    output = None

    for table_file in [s for s in zipped_datafile.namelist() if "e20" in s]:
        with zipped_datafile.open(table_file) as zf:
            table_df = pd.read_csv(zf, header=None, dtype=str)
            table_df.columns = ['data' + str(x) for x in table_df.columns]
        merged_df = geo_dataframe.merge(table_df, left_on='geo4', right_on='data5')
        rng = (
                [merged_df.columns.get_loc('geo48')] +
                list(range(merged_df.columns.get_loc('data6'), len(merged_df.columns)))
        )
        merged_df = merged_df.iloc[:, rng]
        merged_df.columns = ['geo48'] + header[:(len(merged_df.columns) - 1)]
        del header[:(len(merged_df.columns) - 1)]
        if output is not None:
            output = output.merge(merged_df, on="geo48")
        else:
            output = merged_df

    output.rename(columns={"geo48": "FIPS"}, inplace=True)
    output.to_csv(save_path + file_name, index=None, header=True)


def get_bgct_state(state="Georgia", year=2017, span=5, data_path="../data/"):
    """Take state, year, and estimate precision and save denormalized ACS datasets with census tract and block group
    resolution for the given destination."""

    data_file, header_file = download_sf(state, year, span, data_path)
    zipf = zipfile.ZipFile(data_path + data_file)
    geo_file = [s for s in zipf.namelist() if "g20" in s][1]
    with zipf.open(geo_file) as f:
        geo_df = pd.read_csv(f, header=None, dtype=str)

    geo_df.columns = ['geo' + str(x) for x in geo_df.columns]
    bg_geo_df = geo_df[geo_df.iloc[:, 14].notnull()]
    ct_geo_df = geo_df[geo_df.iloc[:, 13].notnull() & geo_df.iloc[:, 14].isnull()]

    code, vname = parse_variable_names(data_path + header_file)
    pd.DataFrame(list(zip(code, vname)), columns=["Code", "Variable name"])\
        .to_csv(data_path + f"acs{year}e{span}_{state}_dictionary.csv", index=None, header=True)

    denormalize_tocsv(ct_geo_df, zipf, code.copy(), data_path, f"acs{year}e{span}_{state}_ct.csv")
    denormalize_tocsv(bg_geo_df, zipf, code.copy(), data_path, f"acs{year}e{span}_{state}_bg.csv")


if __name__ == "__main__":
    get_bgct_state(state="Georgia", year=2017, span=5, data_path="../data/")
