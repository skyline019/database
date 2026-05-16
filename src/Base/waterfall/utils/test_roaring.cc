////
// @file test_roaring.cc
// @brief
// 测试croaring压缩算法
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <roaring/roaring.h>
#include <vector>
#include <set>
#include <random>
#include <algorithm>

namespace wf::utils {

class RoaringBitmapTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 设置随机数生成器
        rng.seed(std::random_device{}());
    }

    void TearDown() override {}

    // 生成随机整数集合
    std::vector<uint32_t>
    generate_random_set(size_t count, uint32_t max_value = 1000000)
    {
        std::set<uint32_t> unique_values;
        std::uniform_int_distribution<uint32_t> dist(0, max_value);

        while (unique_values.size() < count) {
            unique_values.insert(dist(rng));
        }

        return std::vector<uint32_t>(
            unique_values.begin(), unique_values.end());
    }

    std::mt19937 rng;
};

// 测试空bitmap的基本操作
TEST_F(RoaringBitmapTest, EmptyBitmap)
{
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    EXPECT_TRUE(roaring_bitmap_is_empty(bitmap));
    EXPECT_EQ(roaring_bitmap_get_cardinality(bitmap), 0);

    // 测试不存在的值
    EXPECT_FALSE(roaring_bitmap_contains(bitmap, 123));

    roaring_bitmap_free(bitmap);
}

// 测试单个值的添加和查询
TEST_F(RoaringBitmapTest, SingleValue)
{
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    // 添加单个值
    roaring_bitmap_add(bitmap, 42);

    EXPECT_FALSE(roaring_bitmap_is_empty(bitmap));
    EXPECT_EQ(roaring_bitmap_get_cardinality(bitmap), 1);
    EXPECT_TRUE(roaring_bitmap_contains(bitmap, 42));
    EXPECT_FALSE(roaring_bitmap_contains(bitmap, 43));

    roaring_bitmap_free(bitmap);
}

// 测试批量添加和删除
TEST_F(RoaringBitmapTest, BulkOperations)
{
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    // 批量添加值
    std::vector<uint32_t> values = {1, 3, 5, 7, 9, 1000, 50000, 100000};
    for (uint32_t value : values) {
        roaring_bitmap_add(bitmap, value);
    }

    EXPECT_EQ(roaring_bitmap_get_cardinality(bitmap), values.size());

    // 验证所有值都存在
    for (uint32_t value : values) {
        EXPECT_TRUE(roaring_bitmap_contains(bitmap, value));
    }

    // 删除一些值
    roaring_bitmap_remove(bitmap, 3);
    roaring_bitmap_remove(bitmap, 100000);

    EXPECT_EQ(roaring_bitmap_get_cardinality(bitmap), values.size() - 2);
    EXPECT_FALSE(roaring_bitmap_contains(bitmap, 3));
    EXPECT_FALSE(roaring_bitmap_contains(bitmap, 100000));

    roaring_bitmap_free(bitmap);
}

// 测试集合操作：并集、交集、差集
TEST_F(RoaringBitmapTest, SetOperations)
{
    // 创建两个bitmap
    roaring_bitmap_t *bitmap1 = roaring_bitmap_create();
    roaring_bitmap_t *bitmap2 = roaring_bitmap_create();

    // bitmap1: 1, 2, 3, 4, 5
    for (uint32_t i = 1; i <= 5; i++) {
        roaring_bitmap_add(bitmap1, i);
    }

    // bitmap2: 3, 4, 5, 6, 7
    for (uint32_t i = 3; i <= 7; i++) {
        roaring_bitmap_add(bitmap2, i);
    }

    // 并集
    roaring_bitmap_t *union_result = roaring_bitmap_or(bitmap1, bitmap2);
    EXPECT_EQ(roaring_bitmap_get_cardinality(union_result), 7); // 1-7
    for (uint32_t i = 1; i <= 7; i++) {
        EXPECT_TRUE(roaring_bitmap_contains(union_result, i));
    }
    roaring_bitmap_free(union_result);

    // 交集
    roaring_bitmap_t *intersect_result = roaring_bitmap_and(bitmap1, bitmap2);
    EXPECT_EQ(roaring_bitmap_get_cardinality(intersect_result), 3); // 3,4,5
    for (uint32_t i = 3; i <= 5; i++) {
        EXPECT_TRUE(roaring_bitmap_contains(intersect_result, i));
    }
    roaring_bitmap_free(intersect_result);

    // 差集 (bitmap1 - bitmap2)
    roaring_bitmap_t *difference_result =
        roaring_bitmap_andnot(bitmap1, bitmap2);
    EXPECT_EQ(roaring_bitmap_get_cardinality(difference_result), 2); // 1,2
    EXPECT_TRUE(roaring_bitmap_contains(difference_result, 1));
    EXPECT_TRUE(roaring_bitmap_contains(difference_result, 2));
    EXPECT_FALSE(roaring_bitmap_contains(difference_result, 3));
    roaring_bitmap_free(difference_result);

    roaring_bitmap_free(bitmap1);
    roaring_bitmap_free(bitmap2);
}

// 测试迭代器功能
TEST_F(RoaringBitmapTest, Iterator)
{
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    std::vector<uint32_t> values = {10, 20, 30, 1000, 50000};
    for (uint32_t value : values) {
        roaring_bitmap_add(bitmap, value);
    }

    // 方法1：使用roaring_bitmap_to_uint32_array（推荐）
    uint32_t cardinality = roaring_bitmap_get_cardinality(bitmap);
    uint32_t *array = new uint32_t[cardinality];
    roaring_bitmap_to_uint32_array(bitmap, array);

    std::vector<uint32_t> iterated_values(array, array + cardinality);
    delete[] array;

    // 验证迭代结果
    EXPECT_EQ(iterated_values.size(), values.size());
    std::sort(values.begin(), values.end());
    EXPECT_EQ(iterated_values, values);

    roaring_bitmap_free(bitmap);
}

// 测试序列化和反序列化
TEST_F(RoaringBitmapTest, Serialization)
{
    roaring_bitmap_t *original = roaring_bitmap_create();

    // 添加一些测试数据
    std::vector<uint32_t> test_data = generate_random_set(1000, 100000);
    for (uint32_t value : test_data) {
        roaring_bitmap_add(original, value);
    }

    // 序列化
    size_t serialized_size = roaring_bitmap_size_in_bytes(original);
    char *buffer = new char[serialized_size];
    roaring_bitmap_serialize(original, buffer);

    // 反序列化
    roaring_bitmap_t *deserialized = roaring_bitmap_deserialize(buffer);

    // 验证反序列化后的bitmap与原始相同
    EXPECT_EQ(
        roaring_bitmap_get_cardinality(original),
        roaring_bitmap_get_cardinality(deserialized));
    EXPECT_TRUE(roaring_bitmap_equals(original, deserialized));

    // 验证所有值都存在
    for (uint32_t value : test_data) {
        EXPECT_TRUE(roaring_bitmap_contains(deserialized, value));
    }

    delete[] buffer;
    roaring_bitmap_free(original);
    roaring_bitmap_free(deserialized);
}

// 测试压缩效果
TEST_F(RoaringBitmapTest, CompressionEffectiveness)
{
    // 测试连续值的压缩效果
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    // 添加连续范围的值（应该压缩得很好）
    for (uint32_t i = 1000; i < 2000; i++) {
        roaring_bitmap_add(bitmap, i);
    }

    // 添加一些稀疏值
    roaring_bitmap_add(bitmap, 50000);
    roaring_bitmap_add(bitmap, 100000);
    roaring_bitmap_add(bitmap, 150000);

    size_t cardinality = roaring_bitmap_get_cardinality(bitmap);
    size_t size_in_bytes = roaring_bitmap_size_in_bytes(bitmap);

    // 验证压缩效果：存储大小应该远小于原始数据大小
    size_t raw_size = cardinality * sizeof(uint32_t);
    EXPECT_LT(size_in_bytes, raw_size);

    // 计算压缩比
    double compression_ratio = static_cast<double>(size_in_bytes) / raw_size;
    std::cout << "Compression ratio: " << compression_ratio << " ("
              << size_in_bytes << " bytes vs " << raw_size << " bytes)"
              << std::endl;

    roaring_bitmap_free(bitmap);
}

// 测试大规模数据
TEST_F(RoaringBitmapTest, LargeScale)
{
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    // 生成大规模测试数据
    std::vector<uint32_t> large_set = generate_random_set(100000, 10000000);

    // 批量添加
    for (uint32_t value : large_set) {
        roaring_bitmap_add(bitmap, value);
    }

    EXPECT_EQ(roaring_bitmap_get_cardinality(bitmap), large_set.size());

    // 随机抽样验证
    std::shuffle(large_set.begin(), large_set.end(), rng);
    for (size_t i = 0; i < 1000; i++) {
        EXPECT_TRUE(roaring_bitmap_contains(bitmap, large_set[i]));
    }

    // 验证不存在的值
    EXPECT_FALSE(roaring_bitmap_contains(bitmap, 10000001));

    roaring_bitmap_free(bitmap);
}

// 测试边界值
TEST_F(RoaringBitmapTest, BoundaryValues)
{
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    // 测试边界值
    uint32_t boundary_values[] = {0, 65535, 65536, 1000000, UINT32_MAX};

    for (uint32_t value : boundary_values) {
        roaring_bitmap_add(bitmap, value);
        EXPECT_TRUE(roaring_bitmap_contains(bitmap, value));
        roaring_bitmap_remove(bitmap, value);
        EXPECT_FALSE(roaring_bitmap_contains(bitmap, value));
    }

    roaring_bitmap_free(bitmap);
}

// 测试性能（可选）
TEST_F(RoaringBitmapTest, PerformanceBenchmark)
{
    const size_t num_elements = 1000000;
    roaring_bitmap_t *bitmap = roaring_bitmap_create();

    auto start_time = std::chrono::high_resolution_clock::now();

    // 批量添加
    for (uint32_t i = 0; i < num_elements; i += 2) { // 添加一半的元素
        roaring_bitmap_add(bitmap, i);
    }

    auto add_time = std::chrono::high_resolution_clock::now();

    // 查询性能
    size_t found_count = 0;
    for (uint32_t i = 0; i < num_elements; i++) {
        if (roaring_bitmap_contains(bitmap, i)) { found_count++; }
    }

    auto query_time = std::chrono::high_resolution_clock::now();

    auto add_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        add_time - start_time);
    auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        query_time - add_time);

    std::cout << "Add performance: " << add_duration.count() << "ms for "
              << num_elements / 2 << " elements" << std::endl;
    std::cout << "Query performance: " << query_duration.count() << "ms for "
              << num_elements << " queries" << std::endl;
    std::cout << "Found count: " << found_count
              << " (expected: " << num_elements / 2 << ")" << std::endl;

    EXPECT_EQ(found_count, num_elements / 2);

    roaring_bitmap_free(bitmap);
}

} // namespace wf::utils

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}