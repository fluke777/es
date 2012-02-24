require 'es'

describe Es::Field do

  it "should fail if the type is not of an expected type" do
    lambda {Es::Field.new("Id", "xx")}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if the type is not specified" do
    lambda {Es::Field.new("Id", nil)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if the type is not string" do
    lambda {Es::Field.new("Id", [])}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "fields with same name should be same" do
    a = Es::Field.new("Id", "fact")
    b = Es::Field.new("Id", "fact")
    a.should == b
  end

  it "fields with different name should not be same" do
    a = Es::Field.new("Id", "fact")
    b = Es::Field.new("Id2", "fact")
    a.should_not == b
  end

  it "should blow reasonably when parsing string" do
    lambda {Es::Field.parse("")}.should raise_error(Es::InsufficientSpecificationError)
  end
end
