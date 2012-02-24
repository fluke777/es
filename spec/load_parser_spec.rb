require 'pry'
require 'es'

describe Es::Load do

  before :each do
    @spec = [
      {
        :entity => "User",
        :file   => "user_file",
        :fields => [
          {
            :name => "Id",
            :type => "recordid"
          }
        ]
      },
      {
        :entity => "User",
        :file   => "user_2_file",
        :fields => [
          {
            :name => "Name",
            :type => "fact"
          }
        ]
      },
      {
        :entity => "Opportunity",
        :file   => "op_file",
        :fields => [
          {
            :name => "Id",
            :type => "recordid"
          }
        ]
      }
    ]
    @load = Es::Load.parse(@spec)
  end

  it "should parse the spec and return Load instance" do
    
    @load.should be_an_instance_of(Es::Load)
    @load.entities.count.should == 3
    @load.entities.first.should be_an_instance_of(Es::Entity)
    @load.entities.last.name.should == "Opportunity"

    user_entity = @load.entities.first
    user_entity.name.should == "User"
    user_entity.fields.should be_an_instance_of(Array)

    user_entity.fields.first.name.should == "Id"
    user_entity.fields.first.type.should == "recordid"
  end

  it "should compile to json" do
    @load.entities.each do |e|
      JSON.pretty_generate(e.to_load_fragment(1234))
    end
  end

  it "can return a merged entity" do
    entity = @load.get_merged_entity_for("User")
    entity.fields.count == 2
  end

  it "should fail if there are multiple same fields in multiple entities of the same name" do
    spec = [
      {
        :entity => "User",
        :file   => "user_file",
        :fields => [
          {
            :name => "Id",
            :type => "recordid"
          }
        ]
      },
      {
        :entity => "User",
        :file   => "user_2_file",
        :fields => [
          {
            :name => "Id",
            :type => "fact"
          }
        ]
      }
    ]
    lambda{load = Es::Load.parse(spec)}.should raise_error(Es::IncorrectSpecificationError)
  end

end
