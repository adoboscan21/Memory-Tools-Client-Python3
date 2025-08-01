import setuptools

# Lee el archivo README.md para usarlo como la descripción larga del paquete.
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    # Nombre del paquete que se usará para 'pip install'.
    name="memory-tools-client",
    # Versión de tu paquete.
    version="1.0.4",
    # Autor y dirección de correo electrónico.
    author="Adonay Boscan",
    author_email="adoboscan21@gmail.com",
    # Descripción corta del proyecto.
    description="A Python client for the Memory Tools database.",
    # Descripción larga, extraída de README.md.
    long_description=long_description,
    long_description_content_type="text/markdown",
    # URL de tu proyecto (por ejemplo, en GitHub).
    url="https://github.com/adoboscan21/Memory-Tools-Client-Python3.git",
    # Especifica dónde está el código fuente.
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    # Clases de clasificación que ayudan a la gente a encontrar tu proyecto.
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
    ],
    # Dependencias del paquete.
    install_requires=[
    ],
    # Versiones de Python compatibles.
    python_requires=">=3.13.5",
)