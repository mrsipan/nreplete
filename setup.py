import setuptools

setuptools.setup(
    name="nreplete",
    version="0.0.1",
    description="nrepl client",
    author="mrsipan",
    py_modules=["nreplete"],
    package_dir={"": "."},
    install_requires=[
        "bencode2",
        ],
    python_requires=">=3.8",
    )
