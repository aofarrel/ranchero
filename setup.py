from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='ranchero',
    version='0.0.0',
    description="Bioinformatics metadata wrangler",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Ash O'Farrell",
    author_email='aofarrel@ucsc.edu',
    packages=['ranchero'],
    include_package_data=True,
    package_data={"ranchero": ["*.md", "*.pyi"]},
    zip_safe=False,
    url='https://github.com/aofarrel/ranchero.git',
    platforms=["MacOS X", "Posix"],
    license="",
    classifiers=[
        "Programming Language :: Python :: 3.11"
    ]
)
