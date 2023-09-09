#  Copyright 2017-2022 John Snow Labs
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Contains the base classes for Annotator properties."""

from pyspark.ml.param import TypeConverters, Params, Param


class AnnotatorProperties(Params):

    inputAnnotatorTypes = []
    optionalInputAnnotatorTypes = []

    outputAnnotatorType = None

    inputCols = Param(Params._dummy(),
                      "inputCols",
                      "previous annotations columns, if renamed",
                      typeConverter=TypeConverters.toListString)

    outputCol = Param(Params._dummy(),
                      "outputCol",
                      "output annotation column. can be left default.",
                      typeConverter=TypeConverters.toString)

    lazyAnnotator = Param(Params._dummy(),
                          "lazyAnnotator",
                          "Whether this AnnotatorModel acts as lazy in RecursivePipelines",
                          typeConverter=TypeConverters.toBoolean
                          )

    def setInputCols(self, *value):
        """Sets column names of input annotations.

        Parameters
        ----------
        *value : List[str]
            Input columns for the annotator
        """
        if type(value[0]) not in [str, list]:
            raise TypeError("InputCols datatype not supported. It must be either str or list")
        self.inputColsValidation(value)
        return (
            self._set(inputCols=value[0])
            if len(value) == 1 and type(value[0]) == list
            else self._set(inputCols=list(value))
        )

    def inputColsValidation(self, value):
        actual_columns = len(value[0]) if type(value[0]) == list else len(value)
        expected_columns = len(self.inputAnnotatorTypes)

        if len(self.optionalInputAnnotatorTypes) == 0:
            if actual_columns != expected_columns:
                raise TypeError(
                    f"setInputCols in {self.uid} expecting {expected_columns} columns. "
                    f"Provided column amount: {actual_columns}. "
                    f"Which should be columns from the following annotators: {self.inputAnnotatorTypes}")
        else:
            expected_columns += len(self.optionalInputAnnotatorTypes)
            if actual_columns not in [
                len(self.inputAnnotatorTypes),
                expected_columns,
            ]:
                raise TypeError(
                    f"setInputCols in {self.uid} expecting at least {len(self.inputAnnotatorTypes)} columns. "
                    f"Provided column amount: {actual_columns}. "
                    f"Which should be columns from at least the following annotators: {self.inputAnnotatorTypes}")

    def getInputCols(self):
        """Gets current column names of input annotations."""
        return self.getOrDefault(self.inputCols)

    def setOutputCol(self, value):
        """Sets output column name of annotations.

        Parameters
        ----------
        value : str
            Name of output column
        """
        return self._set(outputCol=value)

    def getOutputCol(self):
        """Gets output column name of annotations."""
        return self.getOrDefault(self.outputCol)

    def setLazyAnnotator(self, value):
        """Sets whether Annotator should be evaluated lazily in a
        RecursivePipeline.

        Parameters
        ----------
        value : bool
            Whether Annotator should be evaluated lazily in a
            RecursivePipeline
        """
        return self._set(lazyAnnotator=value)

    def getLazyAnnotator(self):
        """Gets whether Annotator should be evaluated lazily in a
        RecursivePipeline.
        """
        return self.getOrDefault(self.lazyAnnotator)
