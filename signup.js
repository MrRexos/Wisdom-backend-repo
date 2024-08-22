import calls from './API/calls';
import axios from 'axios';

Signup = () =>{
    axios({
    method:'POST',
    url:calls.signup
    })
    }