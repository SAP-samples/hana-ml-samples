from setuptools import setup, find_packages
import os

def get_version():
    with open(os.path.join(os.path.dirname(__file__), 'version.txt')) as ver_file:
        version_str = ver_file.readline().rstrip()
    return version_str

def package_files(directory):
    data_files = []
    for (path, directories, filenames) in os.walk(directory):
        # paths.append((path, filenames))
        files = [os.path.join(path, i) for i in filenames]
        data_files.append((path, files))
        # for filename in filenames:
            # paths.append(os.path.join('..', path, filename))

    return data_files

# Config Data / Mapping data
extra_files = package_files(os.path.join(os.path.dirname(__file__), 'src/hana_ml_artifact/config/data'))
# Base project (HANA) structure
extra_files.extend(package_files(os.path.join(os.path.dirname(__file__), 'src/hana_ml_artifact/generators/templates')))
# Filewriter template files
extra_files.extend(package_files(os.path.join(os.path.dirname(__file__), 'src/hana_ml_artifact/generators/filewriter/templates')))

setup(
    name='hana_ml_artifact',
    version=get_version(),
    description='SAP HANA Python Client SDK extension artifact generation. (Experimental)',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6'
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    data_files=extra_files,
    install_requires=[
        'pandas',
        'hana_ml'
    ],
    python_requires='>=3.4',
    include_package_data=True
)