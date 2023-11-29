from setuptools import setup

setup(
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    name="gconfiglib",
    packages=["gconfiglib"],
    entry_points={"console_scripts": ["cfgctl = gconfiglib.config:main"]},
    install_requires=["kazoo", "pandas"],
    test_suite="nose.collector",
    tests_require=["nose"],
    url="https://github.com/mlyubinin/gconfiglib",
    license="MIT",
    author="Michael Lyubinin",
    author_email="michael@lyubinin.com",
    description="GConfigLib Configuration Validation Library",
    include_package_data=True,
)
