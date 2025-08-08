import setuptools

# Read the README.md file for the long description of the package.
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    # The name of the package that will be used for 'pip install'.
    name="memory-tools-client",
    # The current version of your package.
    version="1.0.9",
    # Author and email address.
    author="Adonay Boscan",
    author_email="adoboscan21@gmail.com",
    # A short description of the project.
    description="A Python client for the Memory Tools database.",
    # The long description, extracted from README.md.
    long_description=long_description,
    long_description_content_type="text/markdown",
    # The URL of your project (e.g., the GitHub repository).
    url="https://github.com/adoboscan21/Memory-Tools-Client-Python3.git",
    # Specifies where the source code is located.
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    # Classifiers that help people find your project on PyPI.
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
    ],
    # Package dependencies required for installation.
    install_requires=[
    ],
    # Compatible Python versions.
    python_requires=">=3.13.5",
)