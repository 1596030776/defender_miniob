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
// Created by WangYunlai on 2022/07/01.
//

#pragma once

#include "sql/operator/physical_operator.h"

/**
 * @brief 选择/投影物理算子
 * @ingroup PhysicalOperator
 */
class ProjectPhysicalOperator : public PhysicalOperator
{
public:
  ProjectPhysicalOperator(std::vector<Field> project_fields, std::vector<std::string> aggregation_names) : project_fields_(project_fields), aggregation_names_(aggregation_names)
  {}

  virtual ~ProjectPhysicalOperator() = default;

  void add_expressions(std::vector<std::unique_ptr<Expression>> &&expressions)
  {
    
  }
  void add_projection(const Table *table, const FieldMeta *field);

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::PROJECT;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  int cell_num() const
  {
    return tuple_.cell_num();
  }

  std::vector<Field> project_fields() {
    return project_fields_;
  }
  std::vector<std::string> aggregation_names() {
    return aggregation_names_;
  }

  Tuple *current_tuple() override;

private:
  ProjectTuple tuple_;
  std::vector<Field> project_fields_;
  std::vector<std::string> aggregation_names_;

  Value max_value_;
  Value min_value_;
  float float_sum_ = 0;
  int int_sum_ = 0;
  int count_ = 0;
};
