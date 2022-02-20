package com.altman.test


/**
 * Created on 2021/11/21 14:43
 * Copyright (c) 2021/11/21,一个很有个性的名字 所有
 *
 * @author : Altman
 */
case class Student(name:String,age:Int)
class CompareDemo[T:Ordering](val v1:T,val v2:T){
  // 这个就是一个正常人能看懂的了，老何是高手
  def comp(implicit ord:Ordering[T]):T = {
    if(ord.gt(v1,v2)) v1 else v2
  }
}
object OrderingDemo{
  implicit object OrderStudent extends Ordering[Student] {
    override def compare(x: Student, y: Student): Int = {
      if (x.age > y.age ) 1 else -1
    }
  }
}
object CompareDemo{
  def main(args: Array[String]): Unit = {
    import OrderingDemo.OrderStudent
    val s1: Student = Student("altman",23)
    val s2: Student = Student("bonnie",24)
    val demo = new CompareDemo[Student](s1,s2)
    val student: Student = demo.comp
    println(student)
    println(methodDemo(1)(2))
  }

  def methodDemo(a:Int)(b:Int):Int = {
    a + b
  }
}

object ArrayBufferDemo {
  def main(args: Array[String]): Unit = {

    methodDemo()("altman")
  }

  def methodDemo()(a:String):Unit = {
    println(a)
  }

}
//object MyImplicit3{
//  implicit class StringImprovement(val s:String) {
//    def increment = s.map(x => (x + 1).toChar)
//    def appendHello:String = s + "Hello"
//  }
//}
//object TypeImplicit3{
//  def main(args: Array[String]): Unit = {
//    val str = "abc"
//    import MyImplicit3.StringImprovement
//    println(str.increment)
//    println(str.appendHello)
//  }
//}