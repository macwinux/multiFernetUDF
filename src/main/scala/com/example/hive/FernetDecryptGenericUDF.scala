package com.example.hive


import com.macasaet.fernet.{Key, Token}
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentTypeException}
import org.apache.hadoop.hive.ql.udf.generic.{GenericUDF, GenericUDFUtils}
import org.apache.hadoop.hive.serde2.objectinspector.{ListObjectInspector, ObjectInspector, ObjectInspectorConverters}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
class FernetDecryptGenericUDF extends GenericUDF{

  private val ARRAY_IDX = 1
  private val VALUE_IDX = 0
  private val ARG_COUNT = 2
  private val FUNC_NAME = "FERNET_DECRYPT"
  private val DEFAULT_OUTPUT = ""
  @transient private var arrayOI: Option[ListObjectInspector] = None
  @transient private var valueOI: Option[StringObjectInspector] = None
  @transient private var arrayElementOI: Option[ObjectInspector] = None
  @transient private var converter: Option[Converter] = None

  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {

    if(arguments.length != 2) {
      throw new UDFArgumentException(s"The function ${FUNC_NAME} accepts ${ARG_COUNT} arguments.")
    }

    if(!arguments(ARRAY_IDX).getCategory.equals(Category.LIST)) {
      throw new UDFArgumentTypeException(ARRAY_IDX,
        s"""
           |"${org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME}" expected at function ${FUNC_NAME},
           | but "${arguments(ARRAY_IDX).getTypeName}" is found
           |""".stripMargin
      )
    }

      arrayOI = Some(arguments(ARRAY_IDX).asInstanceOf[ListObjectInspector])
      arrayElementOI = Some(arrayOI.get.getListElementObjectInspector)
      valueOI = Some(arguments(VALUE_IDX).asInstanceOf[StringObjectInspector])
      val elementObjectInspector = arguments(ARRAY_IDX).asInstanceOf[ListObjectInspector].getListElementObjectInspector
      val returnOI: ObjectInspector = new GenericUDFUtils.ReturnObjectInspectorResolver(true).get(elementObjectInspector)
      converter = Some(ObjectInspectorConverters.getConverter(elementObjectInspector, returnOI))
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val inputVal = if (valueOI.isDefined && arguments(0).get() != null) {
      val value: Object = arguments(VALUE_IDX).get
      valueOI.get.getPrimitiveJavaObject(value)
    } else DEFAULT_OUTPUT
    if (arrayOI.isDefined && arguments.length == 2 && arguments(1).get != null ) {
      Try {
        val array: Object = arguments(ARRAY_IDX).get
        val keys = arrayOI.get.getList(array).asScala.toList.map(x => new Key(x.asInstanceOf[String])).asJava
        val token = Token.fromString(inputVal)
        token.validateAndDecrypt(keys, new Validator)
      } match {
        case Success(v) => v
        case Failure(ex) => inputVal
      }
    } else {
      inputVal
    }

  }

  override def getDisplayString(children: Array[String]): String = {
    assert( children.length == ARG_COUNT)
    return  s"fernet_decrypt(${children(ARRAY_IDX)}, ${children(VALUE_IDX)})"
  }
}
