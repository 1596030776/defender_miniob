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

#include "common/log/log.h"
#include "sql/operator/project_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  RC rc = children_[0]->next();
  Tuple *tuple = children_[0]->current_tuple();

  if (!aggregation_names_.empty()) 
  {
    Value value;
    count_++;
    for (Field field : project_fields_)
    {
      tuple->find_cell(TupleCellSpec(field.table_name(),field.field_name()), value);
    
      if (value.get_string() > max_value_.get_string())
      {
        max_value_ = value;
      }
      if (value.get_string() < min_value_.get_string())
      {
        min_value_ = value;
      }
      if (value.attr_type() == 2)
      {
        int_sum_ += value.get_int();
      } 
      else if (value.attr_type() == 3)
      {
        float_sum_ += value.get_float();
      }
    }
  }
  return rc;
}

RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *ProjectPhysicalOperator::current_tuple()
{
  if (aggregation_names_.empty())
  {
    tuple_.set_tuple(children_[0]->current_tuple());
  }
  else
  {
    ProjectTuple *project_tuple = new ProjectTuple();
    for (int i = 0; i < aggregation_names_.size(); i++)
    {
      std::string aggregation_name = aggregation_names_[i];
      Field project_field = project_fields_[i];
      TupleCellSpec *spec;
      if (aggregation_name == "max") {
        spec = new TupleCellSpec(project_field.table_name(), project_field.field_name(), max_value_.get_string().c_str());
      } else if (aggregation_name == "min") {
        spec = new TupleCellSpec(project_field.table_name(), project_field.field_name(), min_value_.get_string().c_str());
      } else if (aggregation_name == "count") {
        spec = new TupleCellSpec(project_field.table_name(), project_field.field_name(), std::to_string(count_).c_str());
      } else if (aggregation_name == "avg") {
        if (project_field.attr_type() == 2) {
          spec = new TupleCellSpec(project_field.table_name(), project_field.field_name(), std::to_string(int_sum_ / count_).c_str());
        } else {
          spec = new TupleCellSpec(project_field.table_name(), project_field.field_name(), std::to_string(float_sum_ / count_).c_str());
        }
      }
      project_tuple->add_cell_spec(spec);
    }
    
    tuple_.set_tuple(project_tuple);
  }
  
  return &tuple_;
}

void ProjectPhysicalOperator::add_projection(const Table *table, const FieldMeta *field_meta)
{
  // 对单表来说，展示的(alias) 字段总是字段名称，
  // 对多表查询来说，展示的alias 需要带表名字
  TupleCellSpec *spec = new TupleCellSpec(table->name(), field_meta->name(), field_meta->name());
  tuple_.add_cell_spec(spec);
}