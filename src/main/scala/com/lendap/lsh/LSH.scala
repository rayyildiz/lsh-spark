/*
* Copyright (c) 2018 Ramazan AYYILDIZ
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/
package com.lendap.lsh

import com.lendap.spark.lsh.Hasher
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


class LSH(override val uid: String) extends Transformer with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("LSH"))


  /** @group getParam */
  def maxNumberOfPossibleElement = new IntParam(this, "maxNumberOfPossibleElement", "max number of possible elements in a vector (> 0) ",
    ParamValidators.gt(0))

  /** @group getParam */
  def numHashFunc = new IntParam(this, "numH  def setInputCol(value: String): this.type = set(inputCol, value)ashFunc", "number of hash functions (>0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def numHashTables = new IntParam(this, "numHashTables", "number of hashTables (>0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def inputCol = new Param[String](this, "inputCol", "Input Column")

  /** @group getParam */
  def outputCol = new Param[String](this, "outputCol", "Output Column")

  /** @group getParam */
  def getMaxNumberOfPossibleElement: Int = $(maxNumberOfPossibleElement)

  /** @group getParam */
  def getNumHashFunc: Int = $(numHashFunc)

  /** @group getParam */
  def getNumHashTables: Int = $(numHashTables)

  /** @group getParam */
  def setMaxNumberOfPossibleElement(value: Int): this.type = set(maxNumberOfPossibleElement, value)

  /** @group getParam */
  def setNumHashFunc(value: Int): this.type = set(numHashFunc, value)

  /** @group getParam */
  def setNumHashTables(value: Int): this.type = set(numHashTables, value)

  /** @group getParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group getParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)


  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val hasher = Hasher(getMaxNumberOfPossibleElement)

    val t = udf { sv: SparseVector => hasher.hash(sv) }

    val metadata = outputSchema($(outputCol)).metadata

    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[SparseVector], s"This input column must be a SparseVector, but got $inputType")

    val attGroup = new AttributeGroup($(outputCol), 1)
    StructType(schema.fields :+ attGroup.toStructField())
  }
}


object LSH extends DefaultParamsReadable[LSH] {
  override def load(path: String): LSH = super.load(path)
}