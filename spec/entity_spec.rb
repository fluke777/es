require 'es'

describe "MyEntity" do

  it "should generate deleted as an attribute when not in compatibility mode" do
    entity = Es::Entity.create_deleted_entity("deleted", :compatibility_mode => false, :file => "file")
    entity.fields.last.type.should == "attribute"
  end

  it "should generate deleted as an deleted record when in compatibility mode" do
    entity = Es::Entity.create_deleted_entity("deleted", :compatibility_mode => true, :file => "file")
    entity.fields.last.type.should == "isDeleted"
  end

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


end
