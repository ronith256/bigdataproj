class MySuite extends munit.FunSuite {
  test("example test that succeeds") {
    val obtained = 42
    val expected = 42
    assertEquals(obtained, expected)
  }
  test("code completeion"){
    val listofval = List(1,3,4,2,1)
    val k = listofval.reverse
    assertEquals(k, List(1,2,4,3,1))
  }
}

