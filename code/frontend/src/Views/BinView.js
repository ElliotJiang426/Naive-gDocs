import { Table, Button, Space } from 'antd';
import React, {Component} from "react";
import axios from 'axios'
import {url} from '../URLConfig'
import 'antd/dist/antd.css';
import {withRouter} from "react-router-dom";


class BinView extends Component{
    constructor(props) {
        super(props);
        this.state = {
            columns: [
                {
                    title: '文件名',
                    dataIndex: 'name',
                    key: 'name',
                    render: text => <a>{text}</a>,
                },
                {
                    title: '操作',
                    key: 'action',
                    render: (row) => (
                        <Space size="middle">
                            <Button onClick={this.restore(row)}>恢复</Button>
                            <Button onClick={this.delete(row)}>删除</Button>
                        </Space>
                    ),
                },
            ],
        data: [
            {
                name: 'John',
            },
            {
                name: 'Jim',
            },
            {
                name: 'Joe',
            },
        ]
    }
    }

    componentDidMount() {
        axios.get(url+"/getRecycleBin").then(response => {
            console.log(response);
            this.setState({data:response.data})
        }).catch(function (error) {
            console.log(error);
        });
    }


    restore= (row) => () =>{
        console.log(row.name)
        axios.get(url+"/restore?filename="+row.name).then(response => {
            console.log(response);
            window.location.reload()
        }).catch(function (error) {
            console.log(error);
        });
    }

    delete= (row) => () =>{
        console.log(row.name)
        axios.get(url+"/delete?filename="+row.name).then(response => {
            console.log(response);
            window.location.reload()
        }).catch(function (error) {
            console.log(error);
        });
    }

    render(){
        return (
            <div>
                <Table columns={this.state.columns} dataSource={this.state.data} />
            </div>
        );
    }
}
export default withRouter(BinView);
