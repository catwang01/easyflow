from setuptools import setup, find_packages

setup(
        name='EasyFlow',     # 包名字
        version='1.0',   # 包版本
        description='This is an easy workflow',   # 简单描述
        author='edward',  # 作者
        include_package_data=True,    # include everything in source control
        # ...but exclude README.txt from all packages
        packages=find_packages(),
        install_requires=["pyyaml"],
)