/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The main package for uses of KijiScoring.  Contains user facing classes necessary to
 * configure and perform real time scoring.
 *
 * <h3>Classes:</h3>
 * <p>
 *   FreshKijiTableReader: Primary interface for performing fresh reads.  Behaves like a regular
 *   KijiTableReader except for the possibility of freshening.
 * </p>
 * <p>
 *   KijiFreshnessManager: Tool for registering, retrieving, and unregistering freshness policies
 *   for the meta table.
 * </p>
 * <p>
 *   KijiFreshnessPolicy: SPI implemented by the user to perform freshness checks.
 * </p>
 * <p>
 *   PolicyContext: Interface for providing access to request specific contextual information in
 *   KijiFreshnessPolicys.
 * </p>
 * <h3>Packages:</h3>
 * <p>
 *   impl: Contains ApiAudience.Private implementation classes necessary for scoring.
 * </p>
 * <p>
 *   lib: Contains stock implementations of KijiFreshnessPolicys.
 * </p>
 * <p>
 *   tools: Contains command line interface tools for registering and inspecting freshness policies.
 * </p>
 */
package org.kiji.scoring;
