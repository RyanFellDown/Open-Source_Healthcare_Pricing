//This is where we add API definition.
import axios from 'axios'

const API = axios.create({
    baseURL: "http:///localhost:8000"
})

export default API