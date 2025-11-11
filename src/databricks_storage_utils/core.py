__all__ = [
    "DataFrameWriter",
]

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Dict, Tuple, List, Union

if TYPE_CHECKING:
    import pyspark

DEFAULT_OPTIONS = {
    'overwriteSchema': 'true',
}
DEFAULT_PATH = ''

class DataFrameWriter:

    def __init__(
        self,
        default_path: str = None,
        default_mode: str = 'overwrite',
        default_format: str = 'parquet',
        default_options: Dict[str, str] = None,
    ) -> None:
        """
        Initializes a DataFrameWriter object.

        Parameters
        ----------
        default_path : str, optional
            The default path to write to.
            If None, the user must either specify a path or include it in the file's name
            upon calling any write method.
        default_mode : str, optional, default 'overwrite'
            The default write mode.
            Any write method called without specifying a mode will use this default mode.
        default_format : str, optional, default 'parquet'
            The default file format to write in.
            Any write method called without specifying a format will use this default format.
        default_options : Dict, optional, default None
            The default options to pass to the DataFrame writer.
            If None, the default options will be used:
            {
                'overwriteSchema': 'true',
            }
        
        Returns
        -------
        None

        Notes
        -----
        For consistency, the default attributes cannot be changed. However, the user may pass
        different values than those passed upon instancing the class whenever calling a write method.
        """
        if default_path is None:
            self._default_path = DEFAULT_PATH
        else:
            self._default_path = default_path
        
        self._default_mode = default_mode
        self._default_format = default_format

        if default_options is None:
            self._default_options = DEFAULT_OPTIONS
        else:
            self._default_options = default_options

    def print_path_files(
        self,
        spark_session: "pyspark.sql.SparkSession",
        path: str = None,
    ) -> None:
        """
        Prints a list of the files in the specified path.

        Parameters
        ----------
        path : str, optional, default None
            The path to the directory to print.
            If None, `self._default_path` will be used.

        Returns
        -------
        None
        """
        
        path = self._default_path if path is None else path
        print_path_files(spark_session, path)

    def get_path_files(
        self,
        spark_session: "pyspark.sql.SparkSession",
        path = None,
    ) -> List[str]:
        """
        Returns a list of the files in the specified path.

        Parameters
        ----------
        path : str, optional, default None
            The path to the directory to print.
            If None, `self._default_path` will be used.

        Returns
        -------
        None
        """
        path = self._default_path if path is None else path
        files = get_path_files(spark_session, path)
        return files
    
    def delete_path_files(
        self,
        spark_session: "pyspark.sql.SparkSession",
        files: Union[str, List[str]] = None,
        recurse: bool = False,
        path: str = None,
    ) -> bool:
        """
        Deletes the specified files in the specified path.

        Parameters
        ----------
        spark_session : pyspark.sql.SparkSession
            The SparkSession to use.
        path : str, optional, default None
            The path to delete the files from.
            If None, `str` values passed in `files` should contain the path.
        files : List[str], optional, default None
            The files within the specified path to delete.
            If None, `path` should be specified, and the directory itself will be deleted.
        recurse : bool, optional, default False
            Whether to recursively delete the files within a subdirectory.
        """
        path = self._default_path if path is None else path
        delete_path_files(spark_session, path, files, recurse)

    def write(
        self,
        df: "pyspark.sql.DataFrame",
        name: str,
        partition_cols: List["pyspark.sql.Column"] = None,
        path: str = None,
        mode: str = None,
        file_format: str = None,
        options: str = None,
    ) -> None:
        """
        Writes a Spark DataFrame to a file.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            The Spark DataFrame to write.
        name : str
            The name of the file to write.
        partition_cols : List[pyspark.sql.Column], optional, default None
            The column or columns to partition the DataFrame by.
            If None, the DataFrame will not be partitioned.
        path : str, optional, default None
            The path to write the DataFrame to.
            If None, `self._default_path` will be used.
        mode : str, optional, default None
            The write mode.
            If None, `self._default_mode` will be used.
        file_format : str, optional, default None
            The file format to write the DataFrame in.
            If None, `self._default_format` will be used.
        options : Dict, optional, default None
            The options to pass to the DataFrame writer.
            If None, `self._default_options` will be used.
        
        Returns
        -------
        None
        """
        path, mode, file_format, options = self._get_defaults(
            path, mode, file_format, options
        )
        write_file(
            df,
            path + name,
            partition_cols = partition_cols,
            mode = mode,
            file_format = file_format,
            options = options
        )

    def load(
        self,
        spark_session: "pyspark.sql.SparkSession",
        name: str,
        path: str = None,
        mode: str = None,
        file_format: str = None,
        options: Dict[str, str] = None,
    ) -> "pyspark.sql.DataFrame":
        """
        Loads a Spark DataFrame from a file.

        Parameters
        ----------
        spark_session : pyspark.sql.SparkSession
            The SparkSession to use.
        name : str
            The name of the file to load.
        path : str, optional, default None
            The path to the directory where the file `name` is located.
            If None, `self._default_path` will be used.
        mode : str, optional, default None
            Only here for consistency with the `write` method.
        file_format : str, optional, default None
            The format of the file to be loaded.
            If None, `self._default_format` will be used.
        options : Dict, optional, default None
            Only here for consistency with the `write` method.
        """
        path, mode, file_format, options = self._get_defaults(
            path, mode, file_format, options
        )
        df = spark_session.read.format(file_format).load(path+name)
        return df
    
    def write_load(
        self,
        spark_session: "pyspark.sql.SparkSession",
        df: "pyspark.sql.DataFrame",
        name: str,
        partition_cols: List["pyspark.sql.Column"] = None,
        path: str = None,
        mode: str = None,
        file_format: str = None,
        options: str = None,
        write: bool = True,
    ) -> "pyspark.sql.DataFrame":
        """
        Writes a Spark DataFrame to a file and returns a Spark DataFrame loaded from that file.
        Essentially, acts as a checkpoint, allowing the user to write a DataFrame to a file and
        immediately load it back into a Spark DataFrame without having to specify the path.

        Parameters
        ----------
        spark_session : pyspark.sql.SparkSession
            The SparkSession to use.
        df : pyspark.sql.DataFrame
            The Spark DataFrame to write.
        name : str
            The name of the file to write.
        partition_cols : List[pyspark.sql.Column], optional, default None
            The column or columns to partition the DataFrame by.
            If None, the DataFrame will not be partitioned.
        path : str, optional, default None
            The path to write the DataFrame to.
            If None, `self._default_path` will be used.
        mode : str, optional
            The write mode.
            If None, `self._default_mode` will be used.
        file_format : str
            The file format to write the DataFrame in.
            If None, `self._default_format` will be used.
        options : Dict, optional, default None
            The options to pass to the DataFrame writer.
            If None, `self._default_options` will be used.
        write : bool, optional, default True
            Whether to write the DataFrame to a file before loading it back into a Spark DataFrame.

        Returns
        -------
        pyspark.sql.DataFrame
            The Spark DataFrame loaded from the file.
        """
        if write:
            self.write(
                df,
                name,
                partition_cols = partition_cols,
                path = path,
                mode = mode,
                file_format = file_format,
                options = options,
            )
        df = self.load(
            spark_session,
            name,
            path = path,
            file_format = file_format
        )
        return df

    def _get_defaults(
        self,
        path: str,
        mode: str,
        file_format: str,
        options: Dict[str, str],
    ) -> Tuple[str, str, str, Dict[str, str]]:
        """
        Returns the default values for the given parameters if they are None, otherwise returns the given values.

        Parameters
        ----------
        path : str
            The path to write the DataFrame to.
        mode : str
            The write mode.
        file_format : str
            The file format to write the DataFrame in.
        options : Dict
            The options to pass to the DataFrame writer.

        Returns
        -------
        Tuple[str, str, str, Dict[str, str]]
            A tuple containing usable values for the given parameters.
        """
        
        path = self._default_path if path is None else path
        mode = self._default_mode if mode is None else mode
        file_format = self._default_format if file_format is None else file_format
        options = self._default_options if options is None else options

        return path, mode, file_format, options
    
def get_path_files(
    spark_session: "pyspark.sql.SparkSession",
    path: str,
) -> List[str]:
    """
    Returns a list of the files in the specified path.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        The SparkSession to use.
    path : str
        The past within to list the files.

    Returns
    -------
    None
    """
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark_session)
    files = dbutils.fs.ls(path)
    files = [name for path, name, size, ts in files]
    return files

def delete_path_files(
    spark_session: "pyspark.sql.SparkSession",
    path: str = None,
    files: Union[str, List[str]] = None,
    recurse: bool = False,
) -> bool:
    """
    Deletes the specified files in the specified path.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        The SparkSession to use.
    path : str, optional, default None
        The path to delete the files from.
        If None, `str` values passed in `files` should contain the path.
    files : List[str], optional, default None
        The files within the specified path to delete.
        If None, `path` should be specified, and the directory itself will be deleted.
    recurse : bool, optional, default False
        Whether to recursively delete the files within a subdirectory.
    """
    from pyspark.dbutils import DBUtils

    path = '' if path is None else path

    files = '' if files is None else files
    files = [files] if isinstance(files, str) else files

    file_paths = [path+file for file in files]

    dbutils = DBUtils(spark_session)
    for fp in file_paths:
        print(f"Deleting file (recurse={recurse}):")
        print(f"--| {fp}")
        dbutils.fs.rm(fp, recurse=recurse)
    
def print_path_files(
    spark_session: "pyspark.sql.SparkSession",
    path: str,
) -> None:
    """
    Prints the contents of a directory in the Databricks File System (DBFS).

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        The SparkSession to use.
    path : str
        The path to the directory to print the contents of.
    
    Returns
    -------
    None
    """
    
    files = get_path_files(spark_session, path)
    print(path)
    print('./')
    for name in files:
        print(f"    {name}")

def write_file(
    df: "pyspark.sql.DataFrame",
    path: str,
    partition_cols: List["pyspark.sql.Column"] = None,
    mode: str = 'overwrite',
    file_format: str = 'parquet',
    options: str = None,
) -> None:
    """
    Writes a Spark DataFrame to a file.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The Spark DataFrame to write.
    path : str
        The path to write the DataFrame to.
    partition_cols : List[pyspark.sql.Column], optional, default None
        The column or columns to partition the DataFrame by.
        If None, the DataFrame will not be partitioned.
    mode : str, optional, default 'overwrite'
        The write mode.
    file_format : str, optional, default 'parquet'
        The file format to write the DataFrame in.
    options : str, optional, default None
        The options to pass to the DataFrame writer.
        If None, the default options will be used.
    
    Returns
    -------
    None
    """

    if options is None:
        options = DEFAULT_OPTIONS

    df_writer = df.write

    if partition_cols:
        if not isinstance(partition_cols, list):
            partition_cols = [partition_cols]
        
        df_writer = df_writer.partitionBy(*partition_cols)

    df_writer = df_writer.mode(mode)

    for name, value in options.items():
        df_writer = df_writer.option(name, value)
    
    df_writer = df_writer.format(file_format).save(path)

def write_load_file(
    spark_session: "pyspark.sql.SparkSession",
    df: "pyspark.sql.DataFrame",
    path: str,
    write: bool = True,
    file_format: str = 'parquet',
    **kwargs,
) -> "pyspark.sql.DataFrame":
    """
    Writes a Spark DataFrame to a file and returns a new DataFrame loaded from the file.

    Parameters
    ----------
    spark_session : pyspark.sql.SparkSession
        The SparkSession to use.
    df : pyspark.sql.DataFrame
        The Spark DataFrame to write.
    path : str
        The path to write the DataFrame to.
    write : bool, optional, default True
        Whether to write the DataFrame to the file.
    file_format : str, optional, default 'parquet'
        The file format to write the DataFrame in.
    **kwargs
        Additional keyword arguments to pass to the write_file function.
    
    Returns
    -------
    pyspark.sql.DataFrame
        A new DataFrame loaded from the saved file.
    """
    if write:
        write_file(df, path, file_format=file_format, **kwargs)
    
    return spark_session.read.format(file_format).load(path)


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('test').getOrCreate()
    writer = DataFrameWriter()
    print("success")