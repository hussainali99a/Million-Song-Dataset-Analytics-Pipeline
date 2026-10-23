
from pathlib import Path

import h5py

DATA_DIR = Path("data/raw/MillionSongSubset")


def inspect_song_file():
    files = list(DATA_DIR.rglob("*.h5"))

    if not files:
        raise FileNotFoundError(
            f"No HDF5 files found under {DATA_DIR.resolve()}"
        )

    file_path = files[0]
    print(f"Inspecting: {file_path}\n")

    with h5py.File(file_path, "r") as h5:
        def show_item(name, obj):
            if isinstance(obj, h5py.Dataset):
                print(f"DATASET: {name} | shape={obj.shape} | dtype={obj.dtype}")
            elif isinstance(obj, h5py.Group):
                print(f"GROUP:   {name}")

        h5.visititems(show_item)

        print("\n--- Useful metadata ---")
        for group_name, keys in {
            "analysis": ["danceability", "energy"],
            "musicbrainz": ["year"],
            "metadata": ["artist_name", "title", "release"],
            "musicbrainz/artist_mbtags": [],
        }.items():
            if group_name in h5:
                group = h5[group_name]
                for key in keys:
                    if key in group:
                        print(f"{group_name}/{key}: {group[key][()]}")

        if "analysis/songs" in h5:
            print("\nSong record:")
            print(h5["analysis/songs"][()])


if __name__ == "__main__":
    inspect_song_file()
