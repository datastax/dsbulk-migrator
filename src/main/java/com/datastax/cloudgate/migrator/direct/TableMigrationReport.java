/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cloudgate.migrator.direct;

public class TableMigrationReport {

  private final TableMigrator migrator;
  private final ExitStatus status;
  private final String operationId;
  private final boolean export;

  public TableMigrationReport(
      TableMigrator migrator, ExitStatus status, String operationId, boolean export) {
    this.migrator = migrator;
    this.status = status;
    this.operationId = operationId;
    this.export = export;
  }

  public TableMigrator getMigrator() {
    return migrator;
  }

  public boolean isExport() {
    return export;
  }

  public String getOperationId() {
    return operationId;
  }

  public ExitStatus getStatus() {
    return status;
  }
}