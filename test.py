import glob
from multiprocessing import Pool, Process
import os
import shutil
import subprocess

RPMS_FOLDER = "rpms"
EXTRACTED_FOLDER = "extracted"
MANPAGES_FOLDER = "manpages"


class RPMStructure:
    name: str
    path: str
    docs: list[str]

    def __init__(self, path: str) -> None:
        self._update_name(path)
        self._update_path(path)

    def _update_name(self, path: str):
        self.name = path.split("/")[-1].replace(".rpm", "")

    def _update_path(self, path: str):
        self.path = os.path.join(RPMS_FOLDER, path)

    def add_docs(self, docs: list[str]):
        self.docs = docs


def unarchive_rpm_files(rpm: RPMStructure, destination: str):
    rpm2cpio_command = subprocess.Popen(
        ["rpm2cpio", rpm.path],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    with subprocess.Popen(
        ["cpio", "-D", destination, "-idmv"],
        stdin=rpm2cpio_command.stdout,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ):
        rpm2cpio_command.wait()


def gather_doc_files(
    rpm: RPMStructure,
    rpm_extraction_destination: str,
    patterns: tuple[str] = ("/usr/share/doc", "/usr/share/man"),
):
    extracted_files = glob.glob(
        os.path.join(rpm_extraction_destination, "**"), recursive=True
    )
    doc_files = [
        doc_file
        for doc_file in extracted_files
        if any(pattern in doc_file for pattern in patterns) and os.path.isfile(doc_file)
    ]

    rpm.add_docs(doc_files)


def move_doc_files(rpm: RPMStructure, destination: str):
    for doc in rpm.docs:
        cut = f"{EXTRACTED_FOLDER}/{RPMS_FOLDER}/{rpm.name}"
        normalized_doc_name = doc.replace(cut, "")
        final_path = destination + normalized_doc_name
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        shutil.move(doc, final_path)


def list_split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


def worker(all_rpms: list[RPMStructure]):
    for rpm in all_rpms:
        rpm_extraction_destination = f"{EXTRACTED_FOLDER}/{RPMS_FOLDER}/{rpm.name}"
        unarchive_rpm_files(rpm, rpm_extraction_destination)
        gather_doc_files(rpm, rpm_extraction_destination)

        manpage_extraction_destination = (
            f"{EXTRACTED_FOLDER}/{MANPAGES_FOLDER}/{rpm.name}"
        )
        move_doc_files(rpm, manpage_extraction_destination)


def main():
    # Fetch all rpms from the `rpms/` folder
    all_rpms = [RPMStructure(path) for path in os.listdir(RPMS_FOLDER)]
    cpu_count = os.cpu_count()

    print(f"Splitting {len(all_rpms)} rpms across {cpu_count} cores.")

    with Pool(cpu_count) as process:
        process.apply(worker, args=(all_rpms,))


if __name__ == "__main__":
    main()
