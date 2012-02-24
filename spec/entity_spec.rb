require 'es'

describe Es::Entity do

  it "should fail if the file is not defined" do
    opts = {
      :fields => [
        Es::Field.new("User", 'recordid')
      ]
    }
    
    lambda {Es::Entity.new("Id", opts)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if the type is not string" do
    opts = {
      :file => [],
      :fields => [
        Es::Field.new("Id", 'recordid')
      ]
    }
    lambda {Es::Entity.new("User", opts)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if the name is not defined" do
    opts = {
      :file => 'file',
      :fields => [
        Es::Field.new("User", 'recordid')
      ]
    }
    
    lambda {Es::Entity.new(nil, opts)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if the name is not string" do
    opts = {
      :file => 'file',
      :fields => [
        Es::Field.new("Id", 'recordid')
      ]
    }
    lambda {Es::Entity.new([], opts)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if the name is empty" do
    opts = {
      :file => 'file',
      :fields => [
        Es::Field.new("Id", 'recordid')
      ]
    }
    lambda {Es::Entity.new("    ", opts)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if list of fields is not specified" do
    opts = {
      :file => 'file'
    }
    lambda {Es::Entity.new("User", opts)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if list of fields is empty" do
    opts = {
      :file => 'file',
      :fields => []
    }
    lambda {Es::Entity.new("User", opts)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if there are multiple fields with the same name" do
    lambda {
      Es::Entity.new("User", {
        :file => 'file',
        :fields => [
          Es::Field.new("Id", 'recordid'),
          Es::Field.new("Id", 'recordid')
        ]
      })}.should raise_error(Es::IncorrectSpecificationError)
  end
  

end
