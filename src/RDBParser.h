#pragma once

#include <string>

#include "RedisStore.h"

void parseRDB(const std::string& filename, RedisStore& store);
