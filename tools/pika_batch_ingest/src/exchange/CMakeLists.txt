file(GLOB_RECURSE EXCHANGE_SRC CONFIGURE_DEPENDS "*.cpp")
set(EXCHANGE_MAIN "${CMAKE_CURRENT_SOURCE_DIR}/exchange.cpp")
if (EXISTS "${EXCHANGE_MAIN}")
  list(FILTER EXCHANGE_SRC EXCLUDE REGEX ".*/exchange\\.cpp$")
endif()

# ========= 静态库：exchange_lib =========
add_library(exchange_lib STATIC ${EXCHANGE_SRC})

target_include_directories(exchange_lib PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${PROJECT_ROOT_DIR}/src
  ${PROJECT_ROOT_DIR}/src/pstd
  ${PROJECT_ROOT_DIR}/src/pstd/include
)

link_pikiwidb_libs(exchange_lib)

target_link_libraries(exchange_lib PUBLIC pthread z dl)

set_common_definitions(exchange_lib)

# ========= 可执行文件：exchange =========

if(EXISTS "${EXCHANGE_MAIN}")
  add_executable(exchange "${EXCHANGE_MAIN}")
  # 用链接分组把 exchange_lib 和依赖都包起来
  target_link_libraries(exchange PRIVATE
    -Wl,--start-group
    exchange_lib
    ${PIKIWIDB_LIBS}
    -Wl,--end-group
    pthread dl
  )
  set_common_definitions(exchange)
endif()


# 输出路径确认
message(STATUS "PROJECT_ROOT_DIR=${PROJECT_ROOT_DIR}")
message(STATUS "PROJECT_DIR=${PROJECT_DIR}")
message(STATUS "CONFIG_DIR=${CONFIG_DIR}")
