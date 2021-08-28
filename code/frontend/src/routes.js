import SheetView from './Views/SheetView'
import HomeView from  './Views/HomeView'
import BinView from './Views/BinView'

const routes = [
    {
        path: '/file/:sheetID',
        component: SheetView,
        exact: true
    },
    {
        path: '/',
        component: HomeView,
        exact: true
    },
    {
        path: '/bin',
        component: BinView,
        exact: true
    }
]
export default routes;
