/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include <vector>
#include <memory>

#include "common/rc.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class Db;
class Table;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class AggregationStmt : public Stmt 
{
public:
  AggregationStmt() = default;
  ~AggregationStmt() override;

  StmtType type() const override
  {
    return StmtType::AGGREGATION;
  }

public:
  static RC create(Db *db, const AggregationSqlNode &aggregation_sql, Stmt *&stmt);

public:
  Table *&tables() 
  {
    return tables_;
  }
  Field &query_fields() 
  {
    return query_fields_;
  }
  std::string attr_name()
  {
    return attr_name_;
  } 

private:
  Field query_field_;
  Table *table_;
  std::string attr_name_;
};
