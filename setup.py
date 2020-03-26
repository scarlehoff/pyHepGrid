from setuptools import setup, find_packages

setup(
    name="pyHepGrid",
    version="0.9",
    package_dir={"": "src/"},
    packages=find_packages("src"),
    entry_points={
        "console_scripts": [
            "pyHepGrid = pyHepGrid.main:main",
            "pyHepGrid_get_site_info = " "pyHepGrid.extras.get_site_info:main",
        ],
    },
)
