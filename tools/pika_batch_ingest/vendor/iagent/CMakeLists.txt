file(GLOB_RECURSE IAGENT_SRC CONFIGURE_DEPENDS "*.cpp")

set(IAGENT_MAIN "${CMAKE_CURRENT_SOURCE_DIR}/iagent.cpp")
if(EXISTS "${IAGENT_MAIN}")
  list(FILTER IAGENT_SRC EXCLUDE REGEX ".*/iagent\\.cpp$")
endif()

_pick_lib(PIKI_LIB_hiredis hiredis)

find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h
  HINTS
    ${PROJECT_ROOT_DIR}/deps/include
)

add_library(iagent_lib STATIC ${IAGENT_SRC})

target_include_directories(iagent_lib PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(HIREDIS_INCLUDE_DIR)
  target_include_directories(iagent_lib PUBLIC ${HIREDIS_INCLUDE_DIR})
endif()

link_pikiwidb_libs(iagent_lib)

target_link_libraries(iagent_lib PUBLIC ${PIKI_LIB_hiredis})

link_aws_static(iagent_lib)

target_link_libraries(iagent_lib PUBLIC pthread z dl)

set_common_definitions(iagent_lib)

if(EXISTS "${IAGENT_MAIN}")
  add_executable(iagent "${IAGENT_MAIN}")
  target_link_libraries(iagent PRIVATE iagent_lib)
  set_common_definitions(iagent)
endif()

message(STATUS "PROJECT_DIR=${PROJECT_DIR}")
message(STATUS "CONFIG_DIR=${CONFIG_DIR}")
