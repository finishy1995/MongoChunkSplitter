import math
from pymongo import MongoClient
from typing import List
from bson import MinKey
import time


# 常用字符集常量
BINARY_SET = '01'
OCTAL_SET = '01234567'
DECIMAL_SET = '0123456789'
HEXADECIMAL_SET = '0123456789abcdef'
LOWERCASE_SET = 'abcdefghijklmnopqrstuvwxyz'
UPPERCASE_SET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'


def simulate_min_char_len(char_set: str, data_size_mb: int, chunk_size_mb: int, valid_digits: int) -> int:
    base = len(char_set)
    num_chunks = math.ceil(data_size_mb / chunk_size_mb)

    # 计算能够表示从0到最大块数范围内所有值的 char_len
    # 使用 valid_digits 作为起始参考点
    char_len = valid_digits
    while base ** char_len < num_chunks:
        char_len += 1

    # 小数修正值（经验值，TODO）
    char_len *= 2

    return char_len


def sort_char_set(char_set: str) -> str:
    return ''.join(sorted(set(char_set)))  # 对 char_set 按 ANSI 码排序


def split_hex_range(char_set: str, data_size_mb: int, chunk_size_mb: int = 64, valid_digits: int = 5, char_len: int = None) -> List[str]:
    """
    将一个大范围均匀地分割成多个小块，并返回每个块的结束范围的任意进制（char_set）表示，用于 MongoDB 字符串类型的 shard key 分 chunk。

    参数:
    char_set (str): 用于表示数值的字符集，例如 '0123456789abcdef' 表示标准十六进制。也可以使用其他字符集，如 'abcdefghijklmnopqrstuvwxyz' 表示全部小写字母，或 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' 表示全部大小写字母。
    data_size_mb (int): 数据总大小（MB）。
    chunk_size_mb (int): 每个块（chunk）的大小（MB）。默认为 64 MB。
    valid_digits (int): 输出中每个块结束值的有效位数（指前 valid_digits 位的值准确，会填充至 char_len 的长度）。
    char_len (int): 假定的字符集长度。当不确定实际长度或长度很大时，可以将其设置为一个较大的值。默认为 32。如果你不知道怎么设置，可以设置为 None

    返回:
    List[str]: 包含每个块的结束值的列表。

    说明:
    - 该函数假设总数据范围可以用 char_set 的 char_len 次幂表示。
    - 通过将总范围除以块的数量来计算每个块的大小，然后为每个块生成一个结束值。
    - 结束值转换为 char_set 中的字符表示，只保留 valid_digits 指定的位数。
    - 当 char_len 不确定或极大时，可以将其设置为一个较大的值，以确保足够的表示精度。
    - 注意：该函数假设 char_set 的长度能够准确表示所需的进制（例如，十六进制应使用长度为 16 的 char_set）。
    """
    # 按字典序排序
    char_set = sort_char_set(char_set)

    num_chunks = math.ceil(data_size_mb / chunk_size_mb)
    base = len(char_set)

    # 使用经验算法，估算 char_len
    if char_len is None:
        char_len = simulate_min_char_len(char_set, data_size_mb, chunk_size_mb, valid_digits)

    # 计算总范围，基于给定的字符集长度
    total_range = base ** char_len

    # 每个 chunk 应该覆盖的范围大小
    range_per_chunk = total_range / num_chunks

    # 生成 chunk 的结束范围的数组
    end_hexes = []
    for i in range(1, num_chunks):  # 跳过最后一个 chunk
        end = int(range_per_chunk * i) - 1
        end_hex = ''

        # 生成整个数字的字符表示
        while end > 0:
            end_hex = char_set[end % base] + end_hex
            end //= base

        # 补全位数
        end_hex = end_hex.rjust(char_len, char_set[0])

        # 截取前 valid_digits 个字符
        end_hexes.append(end_hex)

    return end_hexes


def get_shard_names(client: MongoClient) -> List[str]:
    """
    从 MongoDB 集群中获取所有分片的名称。

    参数:
    client (MongoClient): 已经配置好的 MongoClient 实例。

    返回:
    List[str]: 包含所有分片名称的列表。
    """
    try:
        result = client.admin.command('listShards')
        return [shard['_id'] for shard in result['shards']]
    except Exception as e:
        print(f'Error during fetching shard names: {e}')
        return []


def execute_command_with_retry(command, client, retry_count=3, retry_delay=0.1):
    """
    执行 MongoDB 命令并在失败时重试。

    参数:
    command (dict): 要执行的 MongoDB 命令。
    client (MongoClient): 已经配置好的 MongoClient 实例。
    retry_count (int): 重试次数。
    retry_delay (float): 重试间隔时间（秒）。
    """
    attempts = 0
    while attempts < retry_count:
        try:
            client.admin.command(command)
            return  # 命令成功执行，退出函数
        except Exception as e:
            attempts += 1
            if attempts == retry_count:
                print(f'Error during executing command: {e}, attempts: {attempts}')
            else:
                time.sleep(retry_delay)  # 等待一段时间后再重试


def perform_splitting(client: MongoClient, database_name: str, collection_name: str, shard_key_field: str, split_keys: List[str], shard_names: List[str] = None):
    """
    使用给定的 MongoClient 连接到 MongoDB，并在指定集合上执行分片操作，然后将分片移动到指定的分片服务器上。

    参数:
    client (MongoClient): 已经配置好的 MongoClient 实例。
    database_name (str): MongoDB 数据库的名称。
    collection_name (str): 需要分片的集合名称。
    shard_key_field (str): 以逗号分隔的分片键字段名字符串。
    split_keys (List[str]): 分片键的分片点数组。
    shard_names (List[str]): 分片名称的列表，用于指定移动每个分片块的目标分片服务器。如果为 None 或长度为 0，则不移动 chunk
    """
    # 将 shard_key_fields 字符串分割成字段名列表
    fields = shard_key_field.split(',')

    num_shards = 0
    if shard_names is not None:
        num_shards = len(shard_names)

    for i, key in enumerate(split_keys):
        # 构建分片命令
        split_command = {
            'split': f'{database_name}.{collection_name}',
            'middle': {field: key if i == 0 else MinKey() for i, field in enumerate(fields)}
        }

        # 执行分片操作
        execute_command_with_retry(split_command, client)

    if num_shards > 0:
        # 构建 find 查询，包含所有分片键字段
        find_query = {field: MinKey() for i, field in enumerate(fields)}
        move_chunk_command = {
            'moveChunk': f'{database_name}.{collection_name}',
            'find': find_query,
            'to': shard_names[0]
        }
        # 执行移动分片操作
        execute_command_with_retry(move_chunk_command, client)

        for i, key in enumerate(split_keys):
            # 分配分片块到循环使用的分片服务器上
            shard_index = (i+1) % num_shards
            shard_name = shard_names[shard_index]
            # 构建 find 查询，包含所有分片键字段
            find_query = {field: key if i == 0 else MinKey() for i, field in enumerate(fields)}

            move_chunk_command = {
                'moveChunk': f'{database_name}.{collection_name}',
                'find': find_query,
                'to': shard_name
            }
            # 执行移动分片操作
            execute_command_with_retry(move_chunk_command, client)

            