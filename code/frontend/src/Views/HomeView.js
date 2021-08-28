import React, {Component} from "react";
import {Button, Input, Modal, Select} from "antd";
import {withRouter} from "react-router-dom";
import axios from "axios";
import {url} from "../URLConfig";
const { Option } = Select;

class HomeView extends Component{
    constructor(props) {
        super(props);
        this.state = {
            modalAddInfoVisible_1: false,
            modalAddInfoVisible_2:false,
            modalAddInfoVisible_3:false,
            buttonName: "文件管理",
            inputData:"",
            op:""
        }
    }

    openModalAddInfo = (type)=>{
        this.setState({modalAddInfoVisible_1: true})
    }

    gotoRecycleBin(){
        this.props.history.push("/bin");
    }

    checkFile=(e)=>{
        this.setState(
            {inputData: e.target.value}
        )
        console.log(this.state.inputData)
    }

    render(){
        return(
            <div className="App">
                <header className="App-header">
                    <Button type="primary" onClick={()=>this.openModalAddInfo("modalAddInfo")}>{this.state.buttonName}</Button>
                    <Modal title="请输入文件名"
                           visible={this.state.modalAddInfoVisible_1}
                           onCancel={()=>{
                               this.setState({modalAddInfoVisible_1: false})
                           }}
                           onOk={()=>{
                               axios.get(url+"/isFileAlive?filename="+this.state.inputData).then(response => {
                                   console.log(response);
                                   this.setState({modalAddInfoVisible_1: false})
                                   if(response.data === false){
                                       this.setState({modalAddInfoVisible_2: true})
                                   }else{
                                       this.setState({modalAddInfoVisible_3: true})
                                   }
                               }).catch(function (error) {
                                   console.log(error);
                               });
                           }}
                    >
                        <span>文件名：</span>
                        <Input onChange={this.checkFile}></Input>
                    </Modal>
                    <Modal title="文件不存在，是否创建"
                           visible={this.state.modalAddInfoVisible_2}
                           onCancel={()=>{
                               this.setState({modalAddInfoVisible_2: false})
                           }}
                           onOk={()=>{
                               axios.get(url+"/create?filename="+this.state.inputData).then(response => {
                                   console.log(response);
                                   this.setState({modalAddInfoVisible_2: false})
                                   this.props.history.push("/file/"+this.state.inputData);
                               }).catch(function (error) {
                                   console.log(error);
                               });
                           }}
                    >
                    </Modal>
                    <Modal title="已找到文件，请问您的选择是"
                           visible={this.state.modalAddInfoVisible_3}
                           onCancel={()=>{
                               this.setState({modalAddInfoVisible_3: false})
                           }}
                           onOk={()=>{
                               if(this.state.ops === "open"){
                                   this.props.history.push("/file/"+this.state.inputData);
                               }
                               else if(this.state.ops === "delete"){
                                   axios.get(url+"/falseDelete?filename="+this.state.inputData).then(response => {
                                           console.log(response);
                                           this.setState({modalAddInfoVisible_3: false})}
                                   ).catch(function (error) {
                                       console.log(error);
                                   })
                               }
                           }}
                    >
                        <Select
                            showSearch
                            style={{ width: 200 }}
                            optionFilterProp="operation"
                            filterOption={(input, option) =>
                                option.operation.toLowerCase().indexOf(input.toLowerCase()) >= 0
                            }
                            onChange={(option)=>this.setState({ops:option})}
                        >
                            <Option value="open">打开文件</Option>
                            <Option value="delete">删除文件</Option>
                        </Select>
                    </Modal>

                    <br></br>
                    <Button type="primary" onClick={this.gotoRecycleBin.bind(this)}>打开回收站</Button>
                </header>
            </div>)
    };
}
export default withRouter(HomeView);;
